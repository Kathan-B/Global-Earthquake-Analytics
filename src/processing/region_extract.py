import os
import sys

# Ensure Spark workers invoke the active Python executable on Windows and Linux
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_region(df: DataFrame, place_col: str = "place") -> DataFrame:
    """
    Derives a standardized 'region' column from the 'place' column.

    Processing logic:
    1. Replaces null or whitespace-only place strings with 'Unknown'.
    2. Strips distance and direction prefixes such as:
       - '10km SW of Ridgecrest, CA' -> 'Ridgecrest, CA'
       - '5.2 km NNE of Los Angeles, CA' -> 'Los Angeles, CA'
       - '39 km ESE of Neiafu, Tonga' -> 'Neiafu, Tonga'
    3. If the string contains one or more commas, extracts the last
       comma-separated token, trimmed of whitespace.
       Note: The extracted region can be a country (e.g. 'Indonesia', 'Tonga'),
       a US state name/abbreviation (e.g. 'CA', 'Alaska', 'Hawaii'), or a territory.
    4. If no comma exists, retains the cleaned area text (e.g. 'Fiji region').
    5. Fallback to 'Unknown' if the resulting text is empty or blank.

    Args:
        df: Input PySpark DataFrame.
        place_col: Name of the input column containing place descriptions (default 'place').

    Returns:
        DataFrame with an added 'region' column (StringType).
    """
    # Regex to match prefixes like '10km SW of ', '5.2 km NNE of ', '15 km of '
    prefix_regex = r"^(?i)\s*\d+(?:\.\d+)?\s*(?:km|mi)\s+(?:(?:[NESW]{1,3}\s+of)|of)\s*"

    # Step 1: Normalize place string and strip distance/direction prefix
    place_clean = F.trim(
        F.regexp_replace(
            F.coalesce(F.col(place_col), F.lit("")),
            prefix_regex,
            ""
        )
    )

    # Step 2: Split by comma
    parts = F.split(place_clean, ",")
    part_count = F.size(parts)
    last_part = F.trim(F.element_at(parts, -1))

    # Step 3: Compute candidate region
    region_expr = (
        F.when(
            (F.col(place_col).isNull()) | (F.trim(F.col(place_col)) == ""),
            F.lit("Unknown")
        )
        .when(part_count > 1,
              F.when(last_part != "", last_part).otherwise(F.lit("Unknown")))
        .when(place_clean != "", place_clean)
        .otherwise(F.lit("Unknown"))
    )

    return df.withColumn("region", region_expr)


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("RegionExtractSmokeTest")
        .getOrCreate()
    )

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
    sample_df = spark.sql(" UNION ALL ".join(test_queries))
    result_df = add_region(sample_df, "place")

    print("\n--- Region Extract Smoke Test ---")
    result_df.show(truncate=False)

    # Collect and verify
    expected = {
        "10km SW of Ridgecrest, CA": "CA",
        "Southern Sumatra, Indonesia": "Indonesia",
        "Fiji region": "Fiji region",
        None: "Unknown",
        "": "Unknown",
        "   ": "Unknown",
        "39 km ESE of Neiafu, Tonga": "Tonga",
        "Maug Islands region, Northern Mariana Islands": "Northern Mariana Islands",
        "95 km SSE of King Cove, Alaska": "Alaska",
    }

    for row in result_df.collect():
        p = row["place"]
        reg = row["region"]
        exp = expected[p]
        assert reg == exp, f"Failed for '{p}': expected '{exp}', got '{reg}'"

    print("All region extraction assertions passed successfully!\n")
    spark.stop()

