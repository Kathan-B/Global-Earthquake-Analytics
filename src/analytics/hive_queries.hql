-- ============================================================================
-- Global Earthquake Analytics - Hive External Table DDL and Queries
-- Database: quakes_db
-- External Table: quakes
-- Storage Format: Parquet
-- Partition Key: year (derived from event timestamp)
-- HDFS Location: /earthquake/clean
-- ============================================================================

CREATE DATABASE IF NOT EXISTS quakes_db;
USE quakes_db;

-- ----------------------------------------------------------------------------
-- External Table DDL
-- Maps directly to the cleaned Parquet dataset written by PySpark.
-- 'year' is defined as the partition column, matching the HDFS partition directories.
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS quakes (
    id STRING,
    ts TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    depth DOUBLE,
    mag DOUBLE,
    mag_type STRING,
    place STRING,
    region STRING,
    month INT,
    hour INT,
    depth_class STRING,
    mag_bucket STRING
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION '/earthquake/clean';

-- Discover and register existing partitions in HDFS
MSCK REPAIR TABLE quakes;

-- ----------------------------------------------------------------------------
-- Query 1: Top 20 Global Seismic Regions by Event Frequency
-- Business Goal: Identify regions with highest earthquake counts and average intensity.
-- ----------------------------------------------------------------------------
SELECT
    region,
    COUNT(*) AS event_count,
    ROUND(AVG(mag), 2) AS avg_magnitude,
    ROUND(MAX(mag), 2) AS max_magnitude
FROM quakes
GROUP BY region
ORDER BY event_count DESC, region ASC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- Query 2: Yearly Seismic Frequency and Magnitude Trends
-- Business Goal: Track longitudinal seismic frequency and intensity changes over time.
-- ----------------------------------------------------------------------------
SELECT
    year,
    COUNT(*) AS total_earthquakes,
    ROUND(AVG(mag), 2) AS avg_magnitude,
    ROUND(MAX(mag), 2) AS max_magnitude
FROM quakes
GROUP BY year
ORDER BY year ASC;

-- ----------------------------------------------------------------------------
-- Query 3: Magnitude Bucket Distribution (Minor, Light, Strong, Major)
-- Business Goal: Assess risk distribution across Richter scale classes.
-- ----------------------------------------------------------------------------
SELECT
    mag_bucket,
    COUNT(*) AS event_count,
    ROUND(AVG(mag), 2) AS avg_magnitude,
    ROUND(MIN(mag), 2) AS min_magnitude,
    ROUND(MAX(mag), 2) AS max_magnitude
FROM quakes
GROUP BY mag_bucket
ORDER BY
    CASE mag_bucket
        WHEN 'minor' THEN 1
        WHEN 'light' THEN 2
        WHEN 'strong' THEN 3
        WHEN 'major' THEN 4
        ELSE 5
    END ASC;

-- ----------------------------------------------------------------------------
-- Query 4: Depth Class vs Magnitude Analysis
-- Business Goal: Understand hypocentral depth classifications (shallow, intermediate, deep)
-- and their relationship to surface magnitude.
-- ----------------------------------------------------------------------------
SELECT
    depth_class,
    COUNT(*) AS event_count,
    ROUND(AVG(mag), 2) AS avg_magnitude,
    ROUND(MAX(mag), 2) AS max_magnitude,
    ROUND(AVG(depth), 2) AS avg_depth_km
FROM quakes
GROUP BY depth_class
ORDER BY
    CASE depth_class
        WHEN 'shallow' THEN 1
        WHEN 'intermediate' THEN 2
        WHEN 'deep' THEN 3
        ELSE 4
    END ASC;

-- ----------------------------------------------------------------------------
-- Query 5: Top 10 Strongest Global Earthquakes
-- Business Goal: Identify extreme historical seismic events for disaster mitigation.
-- ----------------------------------------------------------------------------
SELECT
    id,
    ts,
    mag,
    depth,
    place,
    region,
    year
FROM quakes
ORDER BY mag DESC, ts ASC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- Query 6: Monthly Seasonality Distribution
-- Business Goal: Measure seasonal/monthly distribution of global earthquakes.
-- ----------------------------------------------------------------------------
SELECT
    month,
    COUNT(*) AS monthly_event_count,
    ROUND(AVG(mag), 2) AS avg_magnitude
FROM quakes
GROUP BY month
ORDER BY month ASC;
