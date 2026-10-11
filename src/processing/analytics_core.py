"""
analytics_core.py - Core Earthquake Analytics and Reporting Module.

Provides reusable PySpark analytical functions:
1. top_regions(df, n=20): Event count by region, descending.
2. region_stats(df, min_count=50): Event count, average magnitude, maximum magnitude,
   and average depth for regions exceeding threshold.
3. yearly_trend(df): Event count, average magnitude, and maximum magnitude grouped by year.
4. save_csv(df, path): Exports small aggregate DataFrames to single-part headered CSV.

CLI Interface:
    python src/processing/analytics_core.py --input <parquet-path> --output <output-directory> --min-count <integer>
"""

import argparse
import os
import sys

# Ensure Spark workers invoke active Python environment across platforms
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def top_regions(df: DataFrame, n: int = 20) -> DataFrame:
    """
    Computes top N regions by total earthquake event count, descending.

    Args:
        df: Cleaned earthquake DataFrame with 'region' column.
        n: Number of top regions to return (default 20).

    Returns:
        DataFrame containing [region, event_count].
    """
    return (
        df.groupBy("region")
        .agg(F.count("*").alias("event_count"))
        .orderBy(F.col("event_count").desc(), F.col("region").asc())
        .limit(n)
    )


def region_stats(df: DataFrame, min_count: int = 50) -> DataFrame:
    """
    Computes summary seismic statistics by region for regions with at least min_count events.

    Args:
        df: Cleaned earthquake DataFrame.
        min_count: Minimum event count threshold (default 50; 5 for small samples).

    Returns:
        DataFrame containing [region, event_count, avg_mag, max_mag, avg_depth].
    """
    return (
        df.groupBy("region")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.round(F.max("mag"), 2).alias("max_mag"),
            F.round(F.avg("depth"), 2).alias("avg_depth"),
        )
        .filter(F.col("event_count") >= min_count)
        .orderBy(F.col("event_count").desc(), F.col("region").asc())
    )


def yearly_trend(df: DataFrame) -> DataFrame:
    """
    Computes yearly trend: event count, average magnitude, and maximum magnitude.

    Args:
        df: Cleaned earthquake DataFrame with 'year', 'mag'.

    Returns:
        DataFrame containing [year, event_count, avg_mag, max_mag] sorted by year ascending.
    """
    return (
        df.groupBy("year")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.round(F.max("mag"), 2).alias("max_mag"),
        )
        .orderBy(F.col("year").asc())
    )


def save_csv(df: DataFrame, path: str) -> None:
    """
    Exports a small aggregate DataFrame to a single-part headered CSV.
    Note: Intended strictly for compact analytical aggregates, never full datasets.
    """
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute core earthquake analytics from cleaned Parquet data."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to cleaned Parquet data directory.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Directory to write output aggregate CSV folders.",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=50,
        help="Minimum event count threshold for region stats (default 50; use 5 for small samples).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("EarthquakeAnalyticsCore")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        print(f"Reading cleaned Parquet data from: {args.input}")
        df = spark.read.parquet(args.input)

        out_dir = args.output.rstrip("/\\")

        print("Computing top regions...")
        df_top = top_regions(df, n=20)
        save_csv(df_top, f"{out_dir}/top_regions")

        print(f"Computing region stats (min_count={args.min_count})...")
        df_stats = region_stats(df, min_count=args.min_count)
        save_csv(df_stats, f"{out_dir}/region_stats")

        print("Computing yearly trend...")
        df_trend = yearly_trend(df)
        save_csv(df_trend, f"{out_dir}/yearly_trend")

        print("\n--- Top 5 Regions Preview ---")
        df_top.show(5, truncate=False)

        print("\n--- Yearly Trend Preview ---")
        df_trend.show(10, truncate=False)

        print(f"All core analytics written successfully to: {out_dir}")

    except Exception as e:
        print(f"ERROR: Analytics execution failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
