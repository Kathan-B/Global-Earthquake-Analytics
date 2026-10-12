#!/usr/bin/env python3
"""
kmeans_clustering.py — Spark MLlib KMeans Pipeline for Seismic Pattern Detection.

Person C deliverable.
Performs spherical feature engineering (xyz), standard scaling, KMeans clustering,
and exports point assignments, cluster summaries, sampled points, and the fitted pipeline model.

CLI Usage:
    spark-submit src/analytics/kmeans_clustering.py \
        --input hdfs:///earthquake/clean \
        --output hdfs:///earthquake/ml \
        --k 8 --mode xyz
"""

import argparse
import math
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator


def build_features(df, mode="xyz"):
    """
    Builds feature columns and VectorAssembler for KMeans.
    
    In 'xyz' mode:
        Converts latitude and longitude (degrees) to 3D Cartesian coordinates on unit sphere:
        x = cos(lat_rad) * cos(lon_rad)
        y = cos(lat_rad) * sin(lon_rad)
        z = sin(lat_rad)
        Features: [x, y, z, depth, mag]
    In 'latlon' mode:
        Features: [latitude, longitude, depth, mag]
    """
    # Filter rows with null or invalid values in required columns
    required_cols = ["latitude", "longitude", "depth", "mag"]
    for col in required_cols:
        df = df.filter(F.col(col).isNotNull() & ~F.isnan(F.col(col)))

    if mode == "xyz":
        deg_to_rad = math.pi / 180.0
        df = (
            df.withColumn("lat_rad", F.col("latitude") * F.lit(deg_to_rad))
            .withColumn("lon_rad", F.col("longitude") * F.lit(deg_to_rad))
            .withColumn("feat_x", F.cos("lat_rad") * F.cos("lon_rad"))
            .withColumn("feat_y", F.cos("lat_rad") * F.sin("lon_rad"))
            .withColumn("feat_z", F.sin("lat_rad"))
        )
        feature_cols = ["feat_x", "feat_y", "feat_z", "depth", "mag"]
    elif mode == "latlon":
        feature_cols = ["latitude", "longitude", "depth", "mag"]
    else:
        raise ValueError(f"Invalid mode '{mode}'. Choose 'xyz' or 'latlon'.")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    return df, assembler, feature_cols


def main():
    parser = argparse.ArgumentParser(description="Spark MLlib KMeans Earthquake Clustering")
    parser.add_argument("--input", required=True, help="Input Parquet path (e.g. hdfs:///earthquake/clean)")
    parser.add_argument("--output", required=True, help="Output root path (e.g. hdfs:///earthquake/ml)")
    parser.add_argument("--k", type=int, default=8, help="Number of clusters (default: 8)")
    parser.add_argument("--mode", choices=["xyz", "latlon"], default="xyz", help="Feature mode: xyz or latlon (default: xyz)")
    parser.add_argument("--sample-frac", type=float, default=1.0, help="Fraction of data to sample (default: 1.0)")
    parser.add_argument("--max-iter", type=int, default=30, help="Max iterations for KMeans (default: 30)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")

    args = parser.parse_args()

    if args.k < 2:
        print(f"Error: k must be >= 2, got {args.k}", file=sys.stderr)
        sys.exit(1)

    if args.sample_frac <= 0.0 or args.sample_frac > 1.0:
        print(f"Error: sample-frac must be in (0.0, 1.0], got {args.sample_frac}", file=sys.stderr)
        sys.exit(1)

    spark = (
        SparkSession.builder.appName("Earthquake-KMeans-Clustering")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Loading input data from: {args.input}")
        df = spark.read.parquet(args.input)

        if args.sample_frac < 1.0:
            df = df.sample(withReplacement=False, fraction=args.sample_frac, seed=args.seed)

        df, assembler, feature_cols = build_features(df, mode=args.mode)
        row_count = df.count()
        print(f"Usable records after cleaning: {row_count}")

        if row_count < args.k:
            print(f"Error: row count ({row_count}) is less than k ({args.k})", file=sys.stderr)
            sys.exit(1)

        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
        kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=args.k, maxIter=args.max_iter, seed=args.seed)

        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        print(f"Fitting KMeans pipeline (k={args.k}, mode={args.mode}, maxIter={args.max_iter})...")
        pipeline_model = pipeline.fit(df)

        predictions = pipeline_model.transform(df).cache()

        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette", distanceMeasure="squaredEuclidean")
        silhouette = evaluator.evaluate(predictions)

        print(f"AUDIT: ROWS={row_count} k={args.k} mode={args.mode} silhouette={silhouette:.4f}")

        # 1. Output points: full point-level predictions with cluster assignment
        points_output = f"{args.output}/points"
        print(f"Saving points Parquet to: {points_output}")
        keep_cols = ["id", "ts", "latitude", "longitude", "depth", "mag", "region", "cluster"]
        existing_cols = [c for c in keep_cols if c in predictions.columns]
        predictions.select(existing_cols).write.mode("overwrite").parquet(points_output)

        # 2. Output cluster_summary: one row per cluster
        summary_output = f"{args.output}/cluster_summary"
        print(f"Computing cluster summary...")
        base_summary = (
            predictions.groupBy("cluster")
            .agg(
                F.count("*").alias("quake_count"),
                F.round(F.avg("latitude"), 4).alias("center_lat"),
                F.round(F.avg("longitude"), 4).alias("center_lon"),
                F.round(F.avg("depth"), 2).alias("avg_depth"),
                F.round(F.avg("mag"), 3).alias("avg_mag"),
                F.round(F.max("mag"), 2).alias("max_mag"),
            )
        )

        # Compute top 3 regions per cluster
        if "region" in predictions.columns:
            region_counts = (
                predictions.groupBy("cluster", "region")
                .agg(F.count("*").alias("rcount"))
            )
            from pyspark.sql.window import Window
            w = Window.partitionBy("cluster").orderBy(F.desc("rcount"))
            ranked_regions = (
                region_counts.withColumn("rank", F.row_number().over(w))
                .filter(F.col("rank") <= 3)
            )
            top_regions = (
                ranked_regions.groupBy("cluster")
                .pivot("rank", [1, 2, 3])
                .agg(F.first("region"))
                .withColumnRenamed("1", "top_region_1")
                .withColumnRenamed("2", "top_region_2")
                .withColumnRenamed("3", "top_region_3")
            )
            cluster_summary_df = base_summary.join(top_regions, on="cluster", how="left")
        else:
            cluster_summary_df = (
                base_summary
                .withColumn("top_region_1", F.lit("Unknown"))
                .withColumn("top_region_2", F.lit("Unknown"))
                .withColumn("top_region_3", F.lit("Unknown"))
            )

        cluster_summary_df = cluster_summary_df.orderBy("cluster")
        cluster_summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(summary_output)

        # 3. Output points_sample: reproducible sample capped at 20,000 rows
        sample_output = f"{args.output}/points_sample"
        max_sample_rows = 20000
        if row_count > max_sample_rows:
            sample_fraction = min(1.0, (max_sample_rows * 1.05) / row_count)
            sample_df = predictions.select(existing_cols).sample(withReplacement=False, fraction=sample_fraction, seed=args.seed).limit(max_sample_rows)
        else:
            sample_df = predictions.select(existing_cols)

        print(f"Saving points sample CSV (max {max_sample_rows} rows) to: {sample_output}")
        sample_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(sample_output)

        # 4. Output fitted pipeline model
        model_output = f"{args.output}/model"
        print(f"Saving pipeline model to: {model_output}")
        pipeline_model.write().overwrite().save(model_output)

        predictions.unpersist()
        print("Clustering completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
