"""
analytics_full.py - Full Earthquake Analytics Pipeline (8 Deliverables).

Computes all eight analytical metrics agreed across the data team:
1. top_regions: Top N regions by earthquake frequency.
2. region_stats: Aggregate metrics (count, avg/max magnitude, avg depth) by region.
3. mag_distribution: Magnitude frequency and percentage distribution by mag_bucket.
4. depth_vs_mag: Depth classification (shallow/intermediate/deep) vs magnitude stats.
5. yearly_trend: Annual seismic count, avg magnitude, max magnitude.
6. monthly_trend: Monthly seasonality with a rolling 12-month moving average of event count.
7. strongest_per_decade: Top 5 strongest recorded seismic events per decade.
8. hotspot_grid: Spatial 5-degree latitude/longitude grid cell seismic clustering.

CLI Usage:
    python src/processing/analytics_full.py --input <parquet-path> --output <output-directory> --min-count 50 [--export-csv-dir <output-dir>]
"""

import argparse
import glob
import os
import shutil
import sys

# Ensure Spark workers invoke active Python environment across platforms
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Import core analytics functions
try:
    from src.processing.analytics_core import (
        region_stats,
        save_csv,
        top_regions,
        yearly_trend,
    )
except ImportError:
    from analytics_core import (
        region_stats,
        save_csv,
        top_regions,
        yearly_trend,
    )


def mag_distribution(df: DataFrame) -> DataFrame:
    """
    Computes magnitude distribution across predefined magnitude buckets:
    minor (<3), light (3 to <5), strong (5 to <7), major (>=7).
    """
    total_count = df.count()
    bucket_order = (
        F.when(F.col("mag_bucket") == "minor", 1)
        .when(F.col("mag_bucket") == "light", 2)
        .when(F.col("mag_bucket") == "strong", 3)
        .otherwise(4)
    )

    return (
        df.groupBy("mag_bucket")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.round(F.min("mag"), 2).alias("min_mag"),
            F.round(F.max("mag"), 2).alias("max_mag"),
        )
        .withColumn(
            "percentage",
            F.round((F.col("event_count") / F.lit(total_count)) * 100.0, 2),
        )
        .orderBy(bucket_order.asc())
    )


def depth_vs_mag(df: DataFrame) -> DataFrame:
    """
    Analyzes magnitude and depth relationships across depth classes:
    shallow (<70 km), intermediate (70 to <300 km), deep (>=300 km).
    """
    depth_order = (
        F.when(F.col("depth_class") == "shallow", 1)
        .when(F.col("depth_class") == "intermediate", 2)
        .otherwise(3)
    )

    return (
        df.groupBy("depth_class")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.round(F.min("mag"), 2).alias("min_mag"),
            F.round(F.max("mag"), 2).alias("max_mag"),
            F.round(F.avg("depth"), 2).alias("avg_depth"),
        )
        .orderBy(depth_order.asc())
    )


def monthly_trend(df: DataFrame) -> DataFrame:
    """
    Computes monthly trend by year and month with a 12-month rolling average.
    The rolling average computes the mean event count across the current month and
    preceding 11 months (rowsBetween -11 to 0).
    """
    monthly_agg = (
        df.groupBy("year", "month")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.round(F.max("mag"), 2).alias("max_mag"),
        )
    )

    window_12m = (
        Window.orderBy("year", "month")
        .rowsBetween(-11, 0)
    )

    return (
        monthly_agg
        .withColumn(
            "rolling_avg_12m",
            F.round(F.avg("event_count").over(window_12m), 2),
        )
        .orderBy("year", "month")
    )


def strongest_per_decade(df: DataFrame) -> DataFrame:
    """
    Identifies the top 5 strongest earthquakes per decade using window ranking.
    """
    with_decade = df.withColumn(
        "decade",
        (F.floor(F.col("year") / 10) * 10).cast(IntegerType())
    )

    window_decade = (
        Window.partitionBy("decade")
        .orderBy(F.col("mag").desc(), F.col("ts").asc(), F.col("id").asc())
    )

    ranked = with_decade.withColumn("rank", F.row_number().over(window_decade))

    return (
        ranked.filter(F.col("rank") <= 5)
        .select(
            "decade",
            "rank",
            "id",
            "ts",
            "mag",
            "depth",
            "place",
            "region",
        )
        .orderBy(F.col("decade").desc(), F.col("rank").asc())
    )


def hotspot_grid(df: DataFrame) -> DataFrame:
    """
    Aggregates global earthquake events into 5-degree latitude and longitude grid cells.
    Identifies global seismic hotspots.
    """
    grid_df = df.withColumn(
        "lat_grid",
        F.floor(F.col("latitude") / 5.0) * 5.0
    ).withColumn(
        "lon_grid",
        F.floor(F.col("longitude") / 5.0) * 5.0
    )

    return (
        grid_df.groupBy("lat_grid", "lon_grid")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.round(F.max("mag"), 2).alias("max_mag"),
            F.round(F.avg("depth"), 2).alias("avg_depth"),
        )
        .orderBy(F.col("event_count").desc(), F.col("lat_grid").asc(), F.col("lon_grid").asc())
    )


def export_single_csv(spark_csv_dir: str, target_csv_file: str) -> None:
    """
    Copies the single part-*.csv file written by Spark into a named target CSV file.
    """
    parts = glob.glob(os.path.join(spark_csv_dir, "part-*.csv"))
    if parts:
        os.makedirs(os.path.dirname(os.path.abspath(target_csv_file)), exist_ok=True)
        shutil.copyfile(parts[0], target_csv_file)
        print(f"Exported standalone CSV: {target_csv_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all eight earthquake analytics on cleaned Parquet dataset."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to cleaned Parquet data directory (local or HDFS).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Directory to write the eight aggregate result folders.",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=50,
        help="Minimum event count threshold for region stats (default 50).",
    )
    parser.add_argument(
        "--export-csv-dir",
        default=None,
        help="Optional directory to export compact standalone CSV files (e.g. output/).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("EarthquakeAnalyticsFull")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        print(f"Reading cleaned Parquet data from: {args.input}")
        df = spark.read.parquet(args.input)

        out_dir = args.output.rstrip("/\\")

        # 1. Top Regions
        print("1/8 Computing top regions...")
        df_top = top_regions(df, n=20)
        p_top = f"{out_dir}/top_regions"
        save_csv(df_top, p_top)

        # 2. Region Stats
        print(f"2/8 Computing region stats (min_count={args.min_count})...")
        df_rstats = region_stats(df, min_count=args.min_count)
        p_rstats = f"{out_dir}/region_stats"
        save_csv(df_rstats, p_rstats)

        # 3. Magnitude Distribution
        print("3/8 Computing magnitude distribution...")
        df_mag_dist = mag_distribution(df)
        p_mag_dist = f"{out_dir}/mag_distribution"
        save_csv(df_mag_dist, p_mag_dist)

        # 4. Depth vs Magnitude
        print("4/8 Computing depth class vs magnitude...")
        df_depth_mag = depth_vs_mag(df)
        p_depth_mag = f"{out_dir}/depth_vs_mag"
        save_csv(df_depth_mag, p_depth_mag)

        # 5. Yearly Trend
        print("5/8 Computing yearly trend...")
        df_ytrend = yearly_trend(df)
        p_ytrend = f"{out_dir}/yearly_trend"
        save_csv(df_ytrend, p_ytrend)

        # 6. Monthly Trend with Rolling 12M Average
        print("6/8 Computing monthly trend with rolling 12m average...")
        df_mtrend = monthly_trend(df)
        p_mtrend = f"{out_dir}/monthly_trend"
        save_csv(df_mtrend, p_mtrend)

        # 7. Strongest per Decade
        print("7/8 Computing top 5 strongest earthquakes per decade...")
        df_strongest = strongest_per_decade(df)
        p_strongest = f"{out_dir}/strongest_per_decade"
        save_csv(df_strongest, p_strongest)

        # 8. Hotspot Grid (5-degree cells)
        print("8/8 Computing 5-degree hotspot grid cells...")
        df_hotspot = hotspot_grid(df)
        p_hotspot = f"{out_dir}/hotspot_grid"
        save_csv(df_hotspot, p_hotspot)

        print("\nAll eight analytics successfully generated beneath:", out_dir)

        # Export standalone CSVs if requested
        if args.export_csv_dir:
            export_dir = args.export_csv_dir
            print(f"\nExporting standalone CSV result files to: {export_dir}")
            export_single_csv(p_top, os.path.join(export_dir, "top_regions.csv"))
            export_single_csv(p_rstats, os.path.join(export_dir, "region_stats.csv"))
            export_single_csv(p_mag_dist, os.path.join(export_dir, "mag_distribution.csv"))
            export_single_csv(p_depth_mag, os.path.join(export_dir, "depth_vs_mag.csv"))
            export_single_csv(p_ytrend, os.path.join(export_dir, "yearly_trend.csv"))
            export_single_csv(p_mtrend, os.path.join(export_dir, "monthly_trend.csv"))
            export_single_csv(p_strongest, os.path.join(export_dir, "strongest_per_decade.csv"))
            export_single_csv(p_hotspot, os.path.join(export_dir, "hotspot_grid.csv"))

    except Exception as e:
        print(f"ERROR: Analytics execution failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
