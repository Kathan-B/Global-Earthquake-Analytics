"""
clean_data.py - Reusable PySpark Data Cleaning and Partitioning Pipeline.

Usage:
    python src/processing/clean_data.py --input <input-path> --output <output-path>

Functionality:
1. Validates input existence and required schema columns.
2. Safely parses ISO-8601 UTC timestamps into 'ts'.
3. Performs comprehensive data cleansing:
   - Validates coordinates: latitude in [-90, 90], longitude in [-180, 180]
   - Validates physical bounds: magnitude in [-1, 10], depth in [-10, 800] km
   - Filters event type: retains only 'earthquake' records
   - Deduplicates records on unique event 'id'
4. Derives analytical attributes:
   - 'year', 'month', 'hour' derived from UTC timestamp
   - 'depth_class': 'shallow' (<70km), 'intermediate' (70 to <300km), 'deep' (>=300km)
   - 'mag_bucket': 'minor' (<3), 'light' (3 to <5), 'strong' (5 to <7), 'major' (>=7)
   - 'region': parsed country/state/area via region_extract.add_region
5. Formats and orders columns according to the project data contract.
6. Writes partitioned Parquet files partitioned by 'year' with overwrite mode.
7. Emits an audit summary covering raw, clean, and dropped counts.
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
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Import region extraction helper from processing module
try:
    from src.processing.region_extract import add_region
except ImportError:
    # Fallback if invoked directly within src/processing
    from region_extract import add_region


REQUIRED_COLUMNS = [
    "time",
    "latitude",
    "longitude",
    "depth",
    "mag",
    "place",
    "type",
    "id",
]

OUTPUT_COLUMN_ORDER = [
    "id",
    "ts",
    "latitude",
    "longitude",
    "depth",
    "mag",
    "mag_type",
    "place",
    "region",
    "month",
    "hour",
    "depth_class",
    "mag_bucket",
    "year",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and partition USGS earthquake catalog data into Parquet."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input raw CSV file (local filesystem or HDFS uri).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output Parquet directory (local filesystem or HDFS uri).",
    )
    return parser.parse_args()


def get_spark_session() -> SparkSession:
    """Initialize or reuse SparkSession with UTC timezone configured."""
    return (
        SparkSession.builder
        .appName("CleanEarthquakeData")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def validate_schema(df: DataFrame) -> None:
    """Ensure all required columns exist in the DataFrame; fail fast if missing."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        print(f"ERROR: Missing essential column(s) in input data: {missing}", file=sys.stderr)
        sys.exit(1)


def clean_earthquake_data(df_raw: DataFrame) -> tuple[DataFrame, dict]:
    """
    Cleans raw earthquake DataFrame, derives analytical dimensions, and compiles audit metrics.
    """
    raw_count = df_raw.count()

    # Normalize magType if present
    mag_type_col = (
        F.col("magType").cast(StringType())
        if "magType" in df_raw.columns
        else F.lit(None).cast(StringType())
    )

    # Cast columns and parse UTC timestamp
    prepared_df = df_raw.select(
        F.col("id").cast(StringType()).alias("id"),
        F.coalesce(
            F.to_timestamp(F.col("time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            F.to_timestamp(F.col("time"))
        ).alias("ts"),
        F.col("latitude").cast(DoubleType()).alias("latitude"),
        F.col("longitude").cast(DoubleType()).alias("longitude"),
        F.col("depth").cast(DoubleType()).alias("depth"),
        F.col("mag").cast(DoubleType()).alias("mag"),
        mag_type_col.alias("mag_type"),
        F.col("place").cast(StringType()).alias("place"),
        F.col("type").cast(StringType()).alias("type"),
    )

    # Calculate audit metrics for invalid / non-earthquake data
    invalid_ts = prepared_df.filter(F.col("ts").isNull()).count()
    invalid_coords = prepared_df.filter(
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (~F.col("latitude").between(-90.0, 90.0))
        | (~F.col("longitude").between(-180.0, 180.0))
    ).count()
    invalid_mag_depth = prepared_df.filter(
        F.col("mag").isNull()
        | F.col("depth").isNull()
        | (~F.col("mag").between(-1.0, 10.0))
        | (~F.col("depth").between(-10.0, 800.0))
    ).count()
    non_earthquake = prepared_df.filter(F.col("type") != "earthquake").count()

    # Step 1: Apply primary filters
    valid_df = prepared_df.filter(
        F.col("ts").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
        & F.col("mag").isNotNull()
        & F.col("depth").isNotNull()
        & F.col("mag").between(-1.0, 10.0)
        & F.col("depth").between(-10.0, 800.0)
        & (F.col("type") == "earthquake")
    )

    # Step 2: Deduplicate by unique event id
    count_before_dedup = valid_df.count()
    dedup_df = valid_df.dropDuplicates(["id"])
    clean_count = dedup_df.count()
    duplicate_ids = count_before_dedup - clean_count

    # Step 3: Add derived analytical columns
    with_derived_df = (
        dedup_df
        .withColumn("year", F.year(F.col("ts")).cast(IntegerType()))
        .withColumn("month", F.month(F.col("ts")).cast(IntegerType()))
        .withColumn("hour", F.hour(F.col("ts")).cast(IntegerType()))
        .withColumn(
            "depth_class",
            F.when(F.col("depth") < 70.0, F.lit("shallow"))
            .when((F.col("depth") >= 70.0) & (F.col("depth") < 300.0), F.lit("intermediate"))
            .otherwise(F.lit("deep"))
        )
        .withColumn(
            "mag_bucket",
            F.when(F.col("mag") < 3.0, F.lit("minor"))
            .when((F.col("mag") >= 3.0) & (F.col("mag") < 5.0), F.lit("light"))
            .when((F.col("mag") >= 5.0) & (F.col("mag") < 7.0), F.lit("strong"))
            .otherwise(F.lit("major"))
        )
    )

    # Step 4: Add standardized region
    with_region_df = add_region(with_derived_df, place_col="place")

    # Step 5: Select strictly according to shared data contract
    final_df = with_region_df.select(*OUTPUT_COLUMN_ORDER)

    audit_stats = {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "dropped_rows": raw_count - clean_count,
        "invalid_timestamps": invalid_ts,
        "invalid_coordinates": invalid_coords,
        "invalid_mag_depth": invalid_mag_depth,
        "non_earthquake_events": non_earthquake,
        "duplicate_ids": duplicate_ids,
    }

    return final_df, audit_stats


def print_audit_summary(stats: dict) -> None:
    print("\n" + "=" * 60)
    print("           EARTHQUAKE DATA PROCESSING AUDIT SUMMARY")
    print("=" * 60)
    print(f"RAW ROWS READ:             {stats['raw_rows']:>10,}")
    print(f"CLEAN ROWS WRITTEN:        {stats['clean_rows']:>10,}")
    print(f"TOTAL ROWS DROPPED:        {stats['dropped_rows']:>10,}")
    print("-" * 60)
    print("FILTER DETAILS:")
    print(f"  - Non-earthquake events: {stats['non_earthquake_events']:>10,}")
    print(f"  - Invalid coordinates:   {stats['invalid_coordinates']:>10,}")
    print(f"  - Invalid magnitude/depth:{stats['invalid_mag_depth']:>9,}")
    print(f"  - Invalid timestamps:    {stats['invalid_timestamps']:>10,}")
    print(f"  - Duplicate Event IDs:   {stats['duplicate_ids']:>10,}")
    print("=" * 60 + "\n")


def main() -> None:
    args = parse_args()

    # Validate input argument
    if not args.input or not args.output:
        print("ERROR: Both --input and --output arguments are required.", file=sys.stderr)
        sys.exit(1)

    spark = get_spark_session()

    try:
        print(f"Reading input data from: {args.input}")
        df_raw = spark.read.option("header", "true").csv(args.input)

        # Validate schema
        validate_schema(df_raw)

        # Clean and derive dimensions
        clean_df, audit_stats = clean_earthquake_data(df_raw)

        # Print audit report
        print_audit_summary(audit_stats)

        # Write Parquet partitioned by year with overwrite mode
        print(f"Writing partitioned Parquet output to: {args.output} (partitioned by year)...")
        clean_df.write.mode("overwrite").partitionBy("year").parquet(args.output)
        print("Write completed successfully.")

    except Exception as e:
        print(f"ERROR: Pipeline execution failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
