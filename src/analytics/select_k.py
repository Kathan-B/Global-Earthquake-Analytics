#!/usr/bin/env python3
"""
select_k.py — Elbow (WSSSE / Training Cost) and Silhouette evaluation across candidate k values.

Person C deliverable.
Evaluates KMeans for k ranging from --k-min to --k-max.
Outputs results to a CSV with columns: k, wssse, silhouette.

CLI Usage:
    spark-submit src/analytics/select_k.py \
        --input hdfs:///earthquake/clean \
        --output hdfs:///earthquake/ml/k_selection \
        --k-min 4 --k-max 15 --sample-frac 0.2
"""

import argparse
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Import build_features from kmeans_clustering in same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kmeans_clustering import build_features


def main():
    parser = argparse.ArgumentParser(description="Spark MLlib KMeans Elbow and Silhouette Evaluation")
    parser.add_argument("--input", required=True, help="Input Parquet path (e.g. hdfs:///earthquake/clean)")
    parser.add_argument("--output", required=True, help="Output CSV path for k_selection results")
    parser.add_argument("--k-min", type=int, default=4, help="Minimum k (default: 4)")
    parser.add_argument("--k-max", type=int, default=15, help="Maximum k (default: 15)")
    parser.add_argument("--sample-frac", type=float, default=0.2, help="Sample fraction (default: 0.2)")
    parser.add_argument("--mode", choices=["xyz", "latlon"], default="xyz", help="Feature mode (default: xyz)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--max-iter", type=int, default=20, help="Max iterations per candidate (default: 20)")

    args = parser.parse_args()

    if args.k_min < 2:
        print(f"Error: k-min must be >= 2, got {args.k_min}", file=sys.stderr)
        sys.exit(1)

    if args.k_max < args.k_min:
        print(f"Error: k-max ({args.k_max}) must be >= k-min ({args.k_min})", file=sys.stderr)
        sys.exit(1)

    if args.sample_frac <= 0.0 or args.sample_frac > 1.0:
        print(f"Error: sample-frac must be in (0.0, 1.0], got {args.sample_frac}", file=sys.stderr)
        sys.exit(1)

    spark = (
        SparkSession.builder.appName("Earthquake-Select-K")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Reading dataset from {args.input} with sample fraction {args.sample_frac}...")
        df = spark.read.parquet(args.input)

        if args.sample_frac < 1.0:
            df = df.sample(withReplacement=False, fraction=args.sample_frac, seed=args.seed)

        df, assembler, _ = build_features(df, mode=args.mode)
        raw_df = assembler.transform(df)

        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
        scaler_model = scaler.fit(raw_df)
        prepared_data = scaler_model.transform(raw_df).cache()

        row_count = prepared_data.count()
        print(f"Sampled records for k evaluation: {row_count}")

        if row_count < args.k_max:
            print(f"Error: dataset row count ({row_count}) is smaller than k-max ({args.k_max})", file=sys.stderr)
            sys.exit(1)

        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette", distanceMeasure="squaredEuclidean")

        results = []
        print(f"\nEvaluating k from {args.k_min} to {args.k_max}:")
        print(f"{'k':>4} | {'WSSSE / Cost':>15} | {'Silhouette':>12}")
        print("-" * 38)

        for k in range(args.k_min, args.k_max + 1):
            try:
                kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, maxIter=args.max_iter, seed=args.seed)
                model = kmeans.fit(prepared_data)

                # Training cost is sum of squared distances to nearest center (WSSSE)
                cost = model.summary.trainingCost
                preds = model.transform(prepared_data)
                silhouette = evaluator.evaluate(preds)

                print(f"{k:>4} | {cost:>15.2f} | {silhouette:>12.4f}")
                results.append((k, round(cost, 2), round(silhouette, 4)))
            except Exception as e:
                print(f"Failed evaluation for k={k}: {e}", file=sys.stderr)

        prepared_data.unpersist()

        if not results:
            print("Error: No candidate k values succeeded.", file=sys.stderr)
            sys.exit(1)

        result_df = spark.createDataFrame(results, ["k", "wssse", "silhouette"])
        print(f"\nWriting k evaluation results to: {args.output}")
        result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output)
        print("k evaluation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
