# Earthquake Dataset Quality and Schema Profile

**Dataset Path:** `dataset/quakes.csv` (Full) and `dataset/sample.csv` (10,000 rows + header)  
**Profile Date:** October 2026  
**Author / Role:** Person B (Data Processing, Analytics, and Hive Integration)

---

## 1. Executive Summary

This document profiles the USGS Earthquake Catalog raw dataset utilized in the **Global Earthquake Analytics and Seismic Pattern Detection** project. It documents schema definitions, observed data types, completeness, validity bounds, distributions, and cleansing criteria required for the PySpark pipeline.

---

## 2. Schema and Column Inspection

The raw CSV contains 22 columns defined by the USGS earthquake feed:

| Column | Observed Inferred Type | Null Count (Full) | Null Count (Sample) | Description |
|---|---|---|---|---|
| `time` | String (ISO-8601) | 0 | 0 | Event occurrence timestamp (UTC) |
| `latitude` | Double | 0 | 0 | Latitude in decimal degrees [-90.0, 90.0] |
| `longitude` | Double | 0 | 0 | Longitude in decimal degrees [-180.0, 180.0] |
| `depth` | Double | 0 | 0 | Depth of event in kilometers |
| `mag` | Double | 0 | 0 | Event magnitude |
| `magType` | String | 1 | 0 | Magnitude calculation type (e.g. mb, ml, mw, mwr, md) |
| `nst` | Integer | 127,846 | 4,218 | Number of seismic stations reporting |
| `gap` | Double | 20,420 | 582 | Azimuthal gap between stations in degrees |
| `dmin` | Double | 32,462 | 967 | Horizontal distance to closest station in degrees |
| `rms` | Double | 108 | 2 | Root-mean-square travel time residual in seconds |
| `net` | String | 0 | 0 | Network data contributor code (e.g., us, ak, nc, ci, pr) |
| `id` | String | 0 | 0 | Unique USGS event identifier |
| `updated` | String (ISO-8601) | 0 | 0 | Timestamp when record was last updated |
| `place` | String | 0 | 0 | Textual description of named geographic location |
| `type` | String | 0 | 0 | Seismic event classification |
| `horizontalError` | Double | 22,836 | 635 | Uncertainty in location (km) |
| `depthError` | Double | 13 | 0 | Uncertainty in depth (km) |
| `magError` | Double | 27,257 | 788 | Uncertainty in magnitude |
| `magNst` | Integer | 25,853 | 761 | Number of stations used for magnitude |
| `status` | String | 0 | 0 | Processing status (`reviewed`, `automatic`) |
| `locationSource` | String | 0 | 0 | Network that authored location |
| `magSource` | String | 0 | 0 | Network that authored magnitude |

---

## 3. Dataset Metrics

### 3.1 Row Counts and Duplication
- **Total Records in `dataset/quakes.csv`:** 289,695 rows (excluding header)
- **Total Records in `dataset/sample.csv`:** 10,000 rows (excluding header)
- **Unique Event IDs:** 289,695 (100% unique; 0 duplicate IDs observed in raw data)

### 3.2 Event Type Distribution (`type`)
The project objective is seismic earthquake pattern detection. Records with non-earthquake classifications must be filtered out:

| Event Type | Full Dataset Count | Percentage | Action in Pipeline |
|---|---|---|---|
| `earthquake` | 287,155 | 99.12% | Retained |
| `mining explosion` | 2,136 | 0.74% | Filtered out |
| `ice quake` / `Ice Quake` | 126 | 0.04% | Filtered out |
| `other event` | 99 | 0.03% | Filtered out |
| `volcanic eruption` | 74 | 0.03% | Filtered out |
| `explosion` | 51 | 0.02% | Filtered out |
| `quarry blast` | 24 | 0.01% | Filtered out |
| `mine collapse` | 10 | <0.01% | Filtered out |
| `landslide` / `Landslide` | 8 | <0.01% | Filtered out |
| `experimental explosion` | 5 | <0.01% | Filtered out |
| `sonic boom` | 2 | <0.01% | Filtered out |
| `nuclear explosion` | 1 | <0.01% | Filtered out |
| `collapse` | 1 | <0.01% | Filtered out |
| `induced or triggered event` | 1 | <0.01% | Filtered out |
| `rock burst` | 1 | <0.01% | Filtered out |
| `accidental explosion` | 1 | <0.01% | Filtered out |
| **Total Non-Earthquakes** | **2,540** | **0.88%** | **Dropped** |

---

## 4. Range and Physical Bounds Analysis

The data pipeline enforces explicit geographic and physical validity bounds:

| Metric | Observed Min | Observed Max | Enforced Allowed Range | Compliance |
|---|---|---|---|---|
| **Latitude** | -84.4932° | 87.3860° | [-90.0, 90.0] | 100% compliant |
| **Longitude** | -179.9997° | 179.9994° | [-180.0, 180.0] | 100% compliant |
| **Magnitude (`mag`)** | 0.70 | 8.80 | [-1.0, 10.0] | 100% compliant |
| **Depth (`depth`)** | -3.74 km | 683.58 km | [-10.0, 800.0] km | 100% compliant |

*Note on negative depths:* Slight negative depths (e.g. -3.74 km) represent seismic events hypocentered above mean sea level in mountainous topography. These are physically valid and accepted within the [-10, 800] km project threshold.

---

## 5. Timestamp Format and Parsing

- **Raw Format:** ISO-8601 UTC with fractional seconds, e.g.:
  - `2016-10-04T00:18:31.170Z`
  - `2016-10-04T03:22:52.322Z`
- **Spark Parser Pattern:** `yyyy-MM-dd'T'HH:mm:ss.SSSX`
- **Derived Temporal Partitions:**
  - `year`: extracted as integer (e.g. 2016), used as the Parquet partitioning key.
  - `month`: extracted as integer (1–12) for monthly seasonality analytics.
  - `hour`: extracted as integer (0–23) for diurnal pattern tracking.

---

## 6. Geographic Place and Region Extraction

The `place` column contains varied formats:
1. Distance and direction prefix: `"39 km ESE of Neiafu, Tonga"`
2. Area followed by country/state: `"Maug Islands region, Northern Mariana Islands"`
3. Named region without comma: `"Fiji region"`
4. Distance prefix with state code: `"10km SW of Ridgecrest, CA"`

### Region Extraction Rules:
- Replace null or empty values with `'Unknown'`.
- Strip distance/bearing prefixes matching patterns such as `^\s*\d+(\.\d+)?\s*(km|mi)\s+([NESW]{1,3}\s+of\s+|of\s+)`.
- When a comma exists in the string, extract the substring after the last comma, trimmed of whitespace.
- When no comma exists, retain the cleaned area text.
- Note: Extracted regions represent countries, US state codes (e.g., `CA`, `Alaska`, `Hawaii`), or geographic oceanic regions (e.g., `Fiji region`).

---

## 7. Cleansing Audit Baseline

Expected processing outcomes across the dataset:
- **Raw Rows:** 289,695
- **Expected Non-Earthquake Drops:** 2,540 rows
- **Expected Missing/Out-of-Bound Drops:** 0 rows (all coordinates and magnitudes within limits)
- **Expected Clean Parquet Output Rows:** 287,155 rows
