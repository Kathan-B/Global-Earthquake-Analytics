# Person B Demonstration Script — Earthquake Data Processing, Analytics, and Hive Integration

**Presenter:** Person B (Big Data Engineer — Processing & Analytics)
**Duration:** ~3 minutes
**Workstream:** PySpark Data Cleansing, Region Extraction, Partitioned Parquet Storage, 8 Analytics Deliverables, and Hive SQL Integration

---

## 1. Opening & Hand-off from Person A (0:00 – 0:30)

**Narration:**
> "Thank you, Person A. Now that our 3-node Hadoop cluster is healthy with live DataNodes and NodeManagers across `worker1` and `worker2`, we move to the core data engineering and processing layer.
>
> In HDFS at `/earthquake/raw/quakes.csv`, we have our raw USGS earthquake catalog containing `[RAW_ROW_COUNT, e.g., 289,695]` records spread across 16 MB blocks. Our objective is to clean this data, enforce quality constraints, extract geographical regions, write optimized partitioned Parquet storage, generate eight key seismic analytics, and expose the dataset to Apache Hive."

**Visuals / Actions to Show:**
```bash
docker exec master hdfs dfs -ls -h /earthquake/raw/
docker exec master hdfs dfs -cat /earthquake/raw/quakes.csv | head -3
```

---

## 2. PySpark Data Cleansing & Quality Enforcement (0:30 – 1:15)

**Narration:**
> "We implemented `src/processing/clean_data.py` to process the raw catalog distributedly using PySpark on YARN.
>
> To guarantee data integrity without silent coercion, our pipeline enforces strict physical and coordinate validity bounds:
> 1. Latitude within [-90°, 90°] and Longitude within [-180°, 180°].
> 2. Seismic magnitude within [-1.0, 10.0] and hypocentral depth within [-10 km, 800 km].
> 3. Filtering non-earthquake events (such as quarry blasts and mining explosions), leaving only natural earthquakes.
> 4. Deduplication on USGS event ID.
> 5. ISO-8601 UTC timestamp parsing into a standard `ts` column.
>
> In our audit summary, out of `[RAW_ROWS]` read, `[DROPPED_ROWS]` non-earthquake records were pruned, producing exactly `[CLEAN_ROWS]` clean records."

**Visuals / Actions to Show:**
```bash
# Spark job submission on YARN (or local demo run)
docker exec master spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 1 --executor-memory 1g --driver-memory 1g \
  /opt/project/src/processing/clean_data.py \
  --input hdfs:///earthquake/raw/quakes.csv \
  --output hdfs:///earthquake/clean
```
- Highlight the **DATA QUALITY AUDIT REPORT** printed to stdout.

---

## 3. Region Extraction & Partitioned Parquet Storage (1:15 – 1:50)

**Narration:**
> "A core deliverable is our reusable `region_extract.py` module. Raw USGS place strings frequently have noisy prefixes like `10km SW of Ridgecrest, CA` or `39 km ESE of Neiafu, Tonga`.
>
> Using native Spark SQL regular expressions—without any performance-degrading Python UDFs—we strip distance/bearing prefixes, split on commas, and isolate clean geographical entities: countries, US states, and oceanic seismic regions. We also derive `depth_class` (shallow, intermediate, deep) and `mag_bucket` (minor, light, strong, major).
>
> We write the clean dataset into HDFS at `/earthquake/clean` as Parquet, partitioned by `year`. Parquet provides columnar compression and snappy encoding, while year partitioning enables partition pruning for all time-series queries."

**Visuals / Actions to Show:**
```bash
docker exec master hdfs dfs -ls /earthquake/clean
# Show partition directories: year=2016, year=2017, ...
```

---

## 4. The Eight Seismic Analytics & Exported Deliverables (1:50 – 2:30)

**Narration:**
> "Next, our `src/processing/analytics_full.py` script executes all eight project analytics over the partitioned Parquet data:
> 1. `top_regions`: Frequency ranking of most seismically active global zones.
> 2. `region_stats`: Regional summary with event count, average/max magnitude, and mean depth.
> 3. `mag_distribution`: Event distribution across Richter magnitude buckets with percentage shares.
> 4. `depth_vs_mag`: Depth class comparison against seismic energy release.
> 5. `yearly_trend`: Annual longitudinal frequency and intensity tracking.
> 6. `monthly_trend`: Seasonality tracking featuring a 12-month rolling moving average.
> 7. `strongest_per_decade`: Top 5 historical mega-earthquakes per decade via Spark Window ranking.
> 8. `hotspot_grid`: Global 5-degree latitude/longitude spatial aggregation for density heatmaps.
>
> Compact single-part CSVs for all 8 metrics are exported to `output/*.csv` for Person C's downstream machine learning and MongoDB ingestion."

**Visuals / Actions to Show:**
```bash
head -n 6 output/top_regions.csv
head -n 6 output/mag_distribution.csv
head -n 6 output/strongest_per_decade.csv
```

---

## 5. Hive Integration & Data Contract Verification (2:30 – 3:00)

**Narration:**
> "Finally, we integrate with Apache Hive via `src/analytics/hive_queries.hql`.
>
> We create an external Hive table `quakes_db.quakes` pointing directly to our HDFS Parquet location at `/earthquake/clean`. Notice that `year` is exposed purely as a partition key, avoiding duplicate columns.
>
> After executing `MSCK REPAIR TABLE quakes;` to register the HDFS partitions, we verify our data contract:
> Running `SELECT COUNT(*) FROM quakes_db.quakes;` returns exactly `[CLEAN_ROWS]`, matching our Spark clean count with 100% parity.
>
> Our six analytical HiveQL queries execute as MapReduce jobs on YARN, demonstrating seamless interoperability between Spark, HDFS, and Hive.
>
> With the cleaned data, partitioned Parquet, 8 analytics CSVs, and Hive external tables fully verified, I now hand over to Person C for K-Means clustering in MLlib and MongoDB visualization."

**Visuals / Actions to Show:**
```bash
docker exec master beeline -u jdbc:hive2://master:10000 -f /opt/project/src/analytics/hive_queries.hql
docker exec master beeline -u jdbc:hive2://master:10000 -e "SELECT COUNT(*) FROM quakes_db.quakes;"
```
*(Note: If HiveServer2 is blocked by the known Java 11 classloader issue, point out the HQL script and verify via Spark SQL Parquet count).*

---

## Metric Placeholders Reference Table

| Metric | Sample Dataset (Local Verification) | Full Dataset (Cluster Target) |
|---|---|---|
| Raw Records Read | 10,000 | 289,695 |
| Non-Earthquake Dropped | 94 | 2,540 |
| Invalid Coordinates / Bounds Dropped | 0 | 0 |
| Duplicate IDs Dropped | 0 | 0 |
| Clean Parquet Records Written | 9,906 | 287,155 |
| Hive External Table Count | 9,906 | 287,155 |
