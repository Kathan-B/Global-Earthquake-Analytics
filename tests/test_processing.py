"""
test_processing.py - Comprehensive Unit and Integration Test Suite.

Validates:
1. Region extraction normalization and edge cases without Python UDFs.
2. Clean data pipeline execution on dataset/sample.csv.
3. Schema validation and fail-fast behavior on missing columns/inputs.
4. Duplicate ID removal and coordinate/magnitude boundary filtering.
5. All eight analytics functions (top_regions, region_stats, mag_distribution,
   depth_vs_mag, yearly_trend, monthly_trend, strongest_per_decade, hotspot_grid).
6. Result CSV headers and content validity in output/.
7. Hive external table DDL and query integrity in src/analytics/hive_queries.hql.
"""

import csv
import os
import subprocess
import sys
import tempfile

# Add project root to sys.path so 'src' packages can be imported
sys.path.insert(0, os.path.abspath("."))

import pytest

# Ensure PySpark workers invoke active Python environment across platforms
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.processing.region_extract import add_region
from src.processing.clean_data import clean_earthquake_data, REQUIRED_COLUMNS, OUTPUT_COLUMN_ORDER
from src.processing.analytics_core import top_regions, region_stats, yearly_trend
from src.processing.analytics_full import (
    mag_distribution,
    depth_vs_mag,
    monthly_trend,
    strongest_per_decade,
    hotspot_grid,
)


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession fixture for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("EarthquakeProcessingTestSuite")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_region_extraction_examples(spark):
    """Validate add_region logic with representative examples and edge cases."""
    test_queries = [
        "SELECT '10km SW of Ridgecrest, CA' AS place",
        "SELECT 'Southern Sumatra, Indonesia' AS place",
        "SELECT 'Fiji region' AS place",
        "SELECT CAST(NULL AS STRING) AS place",
        "SELECT '' AS place",
        "SELECT '   ' AS place",
        "SELECT '39 km ESE of Neiafu, Tonga' AS place",
        "SELECT 'Maug Islands region, Northern Mariana Islands' AS place",
        "SELECT '95 km SSE of King Cove, Alaska' AS place",
    ]
    df = spark.sql(" UNION ALL ".join(test_queries))
    res_df = add_region(df, "place")

    rows = {r["place"]: r["region"] for r in res_df.collect()}
    assert rows["10km SW of Ridgecrest, CA"] == "CA"
    assert rows["Southern Sumatra, Indonesia"] == "Indonesia"
    assert rows["Fiji region"] == "Fiji region"
    assert rows[None] == "Unknown"
    assert rows[""] == "Unknown"
    assert rows["   "] == "Unknown"
    assert rows["39 km ESE of Neiafu, Tonga"] == "Tonga"
    assert rows["Maug Islands region, Northern Mariana Islands"] == "Northern Mariana Islands"
    assert rows["95 km SSE of King Cove, Alaska"] == "Alaska"


def test_clean_data_valid_sample(spark):
    """Test cleaning pipeline on sample dataset and verify schema/row counts."""
    sample_csv = "dataset/sample.csv"
    assert os.path.exists(sample_csv), f"Sample CSV file not found: {sample_csv}"

    df_raw = spark.read.option("header", "true").csv(sample_csv)
    clean_df, audit_stats = clean_earthquake_data(df_raw)

    assert audit_stats["raw_rows"] == 10000
    assert audit_stats["non_earthquake_events"] == 94
    assert audit_stats["clean_rows"] == 9906
    assert audit_stats["dropped_rows"] == 94
    assert audit_stats["duplicate_ids"] == 0

    # Verify column count and order
    assert clean_df.columns == OUTPUT_COLUMN_ORDER

    # Verify only earthquake records retained
    non_eq = clean_df.filter(F.col("depth_class").isNull()).count()
    assert non_eq == 0


def test_clean_data_missing_column_fails():
    """Verify that clean_data.py exits with non-zero status when a required column is missing."""
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv", newline="") as f:
        writer = csv.writer(f)
        # Missing 'latitude' and 'longitude'
        writer.writerow(["time", "depth", "mag", "place", "type", "id"])
        writer.writerow(["2016-10-04T00:18:31.170Z", "10.0", "4.5", "Tonga", "earthquake", "id1"])
        temp_csv = f.name

    try:
        cmd = [
            sys.executable,
            "src/processing/clean_data.py",
            "--input", temp_csv,
            "--output", tempfile.mkdtemp(),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode != 0
        assert "Missing essential column(s)" in result.stderr
    finally:
        if os.path.exists(temp_csv):
            os.remove(temp_csv)


def test_clean_data_missing_input_file_fails():
    """Verify that clean_data.py exits with non-zero status when input file does not exist."""
    cmd = [
        sys.executable,
        "src/processing/clean_data.py",
        "--input", "non_existent_file.csv",
        "--output", tempfile.mkdtemp(),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode != 0


def test_duplicate_id_and_out_of_bounds_filtering(spark):
    """Verify deduplication by ID and strict filtering on coordinates and bounds."""
    # Construct a synthetic DataFrame with invalid rows
    rows_sql = [
        # Valid row 1
        "SELECT 'id1' AS id, '2016-10-04T00:00:00.000Z' AS time, 10.0 AS latitude, 20.0 AS longitude, 15.0 AS depth, 4.5 AS mag, 'mb' AS magType, 'Place 1, CA' AS place, 'earthquake' AS type",
        # Duplicate of row 1 (same id)
        "SELECT 'id1' AS id, '2016-10-04T00:00:00.000Z' AS time, 10.0 AS latitude, 20.0 AS longitude, 15.0 AS depth, 4.5 AS mag, 'mb' AS magType, 'Place 1, CA' AS place, 'earthquake' AS type",
        # Invalid latitude (> 90)
        "SELECT 'id2' AS id, '2016-10-04T00:00:00.000Z' AS time, 95.0 AS latitude, 20.0 AS longitude, 15.0 AS depth, 4.5 AS mag, 'mb' AS magType, 'Place 2, CA' AS place, 'earthquake' AS type",
        # Invalid longitude (< -180)
        "SELECT 'id3' AS id, '2016-10-04T00:00:00.000Z' AS time, 10.0 AS latitude, -185.0 AS longitude, 15.0 AS depth, 4.5 AS mag, 'mb' AS magType, 'Place 3, CA' AS place, 'earthquake' AS type",
        # Invalid magnitude (> 10)
        "SELECT 'id4' AS id, '2016-10-04T00:00:00.000Z' AS time, 10.0 AS latitude, 20.0 AS longitude, 15.0 AS depth, 11.0 AS mag, 'mb' AS magType, 'Place 4, CA' AS place, 'earthquake' AS type",
        # Non-earthquake event
        "SELECT 'id5' AS id, '2016-10-04T00:00:00.000Z' AS time, 10.0 AS latitude, 20.0 AS longitude, 15.0 AS depth, 4.5 AS mag, 'mb' AS magType, 'Place 5, CA' AS place, 'mining explosion' AS type",
    ]
    df_raw = spark.sql(" UNION ALL ".join(rows_sql))
    clean_df, stats = clean_earthquake_data(df_raw)

    assert stats["raw_rows"] == 6
    assert stats["clean_rows"] == 1  # Only id1 should survive
    assert stats["duplicate_ids"] == 1
    assert stats["invalid_coordinates"] == 2
    assert stats["invalid_mag_depth"] == 1
    assert stats["non_earthquake_events"] == 1


def test_all_eight_analytics(spark):
    """Test all eight analytics computations on cleaned sample data."""
    sample_csv = "dataset/sample.csv"
    df_raw = spark.read.option("header", "true").csv(sample_csv)
    clean_df, _ = clean_earthquake_data(df_raw)

    # 1. top_regions
    df_top = top_regions(clean_df, n=5)
    assert df_top.count() == 5
    assert set(df_top.columns) == {"region", "event_count"}

    # 2. region_stats
    df_rstats = region_stats(clean_df, min_count=5)
    assert df_rstats.count() > 0
    assert set(df_rstats.columns) == {"region", "event_count", "avg_mag", "max_mag", "avg_depth"}

    # 3. mag_distribution
    df_mag = mag_distribution(clean_df)
    assert df_mag.count() == 4  # minor, light, strong, major
    assert "percentage" in df_mag.columns

    # 4. depth_vs_mag
    df_dmag = depth_vs_mag(clean_df)
    assert df_dmag.count() <= 3  # shallow, intermediate, deep
    assert "avg_depth" in df_dmag.columns

    # 5. yearly_trend
    df_ytrend = yearly_trend(clean_df)
    assert df_ytrend.count() >= 1
    assert set(df_ytrend.columns) == {"year", "event_count", "avg_mag", "max_mag"}

    # 6. monthly_trend
    df_mtrend = monthly_trend(clean_df)
    assert df_mtrend.count() >= 1
    assert "rolling_avg_12m" in df_mtrend.columns

    # 7. strongest_per_decade
    df_strong = strongest_per_decade(clean_df)
    assert df_strong.count() <= 5
    assert "rank" in df_strong.columns

    # 8. hotspot_grid
    df_grid = hotspot_grid(clean_df)
    assert df_grid.count() > 0
    assert "lat_grid" in df_grid.columns
    assert "lon_grid" in df_grid.columns


def test_output_csv_files_exist_and_non_empty():
    """Verify that all eight required result CSV files exist in output/ with valid content."""
    expected_files = [
        "top_regions.csv",
        "region_stats.csv",
        "mag_distribution.csv",
        "depth_vs_mag.csv",
        "yearly_trend.csv",
        "monthly_trend.csv",
        "strongest_per_decade.csv",
        "hotspot_grid.csv",
    ]
    for fname in expected_files:
        fpath = os.path.join("output", fname)
        assert os.path.exists(fpath), f"Expected output CSV missing: {fpath}"
        assert os.path.getsize(fpath) > 50, f"Output CSV unexpectedly small/empty: {fpath}"
        with open(fpath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
            assert len(header) > 0, f"Empty header in {fpath}"
            assert len(rows) > 0, f"No data rows in {fpath}"


def test_hive_hql_script_integrity():
    """Verify HiveQL script contains external table DDL matching schema and partition contract."""
    hql_path = "src/analytics/hive_queries.hql"
    assert os.path.exists(hql_path), f"Hive HQL file missing: {hql_path}"

    with open(hql_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "CREATE DATABASE IF NOT EXISTS quakes_db;" in content
    assert "USE quakes_db;" in content
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS quakes" in content
    assert "PARTITIONED BY (year INT)" in content
    assert "STORED AS PARQUET" in content
    assert "LOCATION '/earthquake/clean'" in content
    assert "MSCK REPAIR TABLE quakes;" in content

    # Check that year is NOT duplicated in column list
    # The columns before PARTITIONED BY must not contain "year INT"
    table_ddl = content[content.find("CREATE EXTERNAL TABLE"):content.find("PARTITIONED BY")]
    assert "year " not in table_ddl, "Column 'year' should not be duplicated in table body"

    # Verify presence of all 6 queries
    assert "Query 1:" in content
    assert "Query 2:" in content
    assert "Query 3:" in content
    assert "Query 4:" in content
    assert "Query 5:" in content
    assert "Query 6:" in content
