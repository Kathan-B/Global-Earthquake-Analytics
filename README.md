# Global Earthquake Analytics and Seismic Pattern Detection

[![Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-yellow.svg)](https://hadoop.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-red.svg)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green.svg)](https://www.mongodb.com/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)

Distributed Big Data Analytics system for ingesting, processing, clustering, and visualizing global earthquake records using a 3-node Apache Hadoop cluster, PySpark distributed compute, MLlib spatial K-Means clustering, and MongoDB.

---

## 1. Problem Statement

Earthquakes pose catastrophic humanitarian and economic risks globally, yet raw seismological event streams are massive, multi-dimensional, and dispersed across uneven spatial geometries. This project develops a fault-tolerant, distributed Big Data analytics pipeline that transforms over 289,000 global seismic event records into actionable spatial clusters, identifies high-magnitude tectonic fault zones, and provides interactive geospatial query interfaces for disaster preparedness.

---

## 2. Architecture & Data Flow

```
+----------------------------------------------------------------------------------------------------+
|                                    END-TO-END DATA ARCHITECTURE                                     |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|   +-------------------+                                                                            |
|   | USGS Catalog CSV  |                                                                            |
|   |  (289,695 events) |                                                                            |
|   +---------+---------+                                                                            |
|             | [hdfs dfs -put]                                                                      |
|             v                                                                                      |
|   +---------------------------------------+                                                        |
|   | HDFS Raw Storage (/earthquake/raw/)   | (Replication: 2, Block: 16 MB)                         |
|   +-------------------+-------------------+                                                        |
|                       |                                                                            |
|                       v [PySpark on YARN / clean_data.py]                                          |
|   +---------------------------------------+                                                        |
|   | HDFS Cleaned Parquet                  | (Partitioned by 'year', filtered & standardized)       |
|   | (/earthquake/clean/year=YYYY/)        |                                                        |
|   +---------+--------------------+--------+                                                        |
|             |                    |                                                                 |
|             |                    +--------------------------------+                                |
|             v [analytics_full.py / Hive DDL]                      v [select_k.py & kmeans.py]      |
|   +---------------------------------------+      +-----------------------------------------+       |
|   | Spatial & Temporal Aggregations       |      | Spark MLlib Spherical KMeans Pipeline   |       |
|   | (Top regions, monthly/yearly trends,  |      | • Feature Space: [x, y, z, depth, mag]  |       |
|   |  hotspot grid, depth classes)         |      | • StandardScaler + KMeans (k=8)         |       |
|   +-------------------+-------------------+      +--------------------+--------------------+       |
|                       |                                               |                            |
|                       +-----------------------+-----------------------+                            |
|                                               | [Export to output/*.csv]                           |
|                                               v                                                    |
|                              +---------------------------------+                                   |
|                              | MongoDB Database ('quakes')     |                                   |
|                              | • clusters, cluster_points      |                                   |
|                              | • region_stats, monthly_trends  |                                   |
|                              +----------------+----------------+                                   |
|                                               |                                                    |
|                                               v                                                    |
|                              +---------------------------------+                                   |
|                              | Visualizations & Analytics      |                                   |
|                              | • Folium Interactive Map (.html)|                                   |
|                              | • Publication Figures (.png)    |                                   |
|                              | • Jupyter Notebook (.ipynb)     |                                   |
|                              +---------------------------------+                                   |
+----------------------------------------------------------------------------------------------------+
```

---

## 3. Cluster Setup (1 Master + 2 Workers)

The distributed processing infrastructure is provisioned as four coordinated Docker containers connected via a private bridge network:

| Node | Hostname | Role | Assigned Daemons & Services | Port Mappings |
|:---|:---|:---|:---|:---|
| **Master** | `master` | Cluster Controller & Driver | HDFS NameNode, YARN ResourceManager, Spark Driver (Client mode), HiveServer2 | `9870` (HDFS UI), `8088` (YARN UI), `4040` (Spark UI), `10000` (Hive) |
| **Worker 1** | `worker1` | Distributed Worker Node | HDFS DataNode, YARN NodeManager (Spark Executor) | `9864` (DataNode UI) |
| **Worker 2** | `worker2` | Distributed Worker Node | HDFS DataNode, YARN NodeManager (Spark Executor) | `9864` (DataNode UI) |
| **Mongo** | `mongo` | Analytics Document Store | MongoDB 7.0 Community Daemon | `27017` (Mongo URI) |

**Storage & Compute Configuration:**
- Hadoop: `3.3.6`, Java: `11`, Spark: `3.5.1`, Hive: `3.1.3`
- HDFS Block Size: `16,777,216 bytes` (16 MB), Replication Factor: `2`
- YARN Resource Allocation: 2 executors, 1 core per executor, 1 GB memory per executor, 8 shuffle partitions.

---

## 4. Dataset Overview

- **Source:** United States Geological Survey (USGS) Earthquake Hazards Catalog
- **Total Processed Records:** 289,695 earthquakes (2016 to 2026)
- **Magnitude Span:** M0.7 to M8.8 (Great Megathrusts)
- **Depth Span:** 0.0 km to 683.6 km (Crustal shallow to transition-zone deep mantle)
- **Primary Schema Fields:** `id`, `ts`, `latitude`, `longitude`, `depth`, `mag`, `mag_type`, `place`, `region`, `depth_class`, `mag_bucket`, `year`, `month`, `hour`.

---

## 5. Pipeline Stages & Execution

The end-to-end pipeline is automated via `pipeline/run_pipeline.sh`:

1. **Stage 1 — HDFS Storage Ingestion:** Confirms `/earthquake/raw/quakes.csv` exists and verifies HDFS block distribution across both DataNodes.
2. **Stage 2 — Distributed Cleansing (Person B):** Removes missing coordinates, standardizes UTC timestamps, parses geographic region names, categorizes depth and magnitude classes, and writes Parquet partitioned by `year`.
3. **Stage 3 — Core Aggregations (Person B):** Generates region statistics, 1-degree hotspot grid cells, yearly distributions, and 12-month rolling averages.
4. **Stage 4 — Hive Analytics (Person B & A):** External Parquet table mapping with HiveQL partitioning (executed conditionally if HiveServer2 is active).
5. **Stage 5 — MLlib $k$-Selection (Person C):** Evaluates Within-Set Sum of Squared Errors (elbow method) and Silhouette scores for $k \in [4, 12]$.
6. **Stage 6 — Spherical K-Means Clustering (Person C):** Trains an MLlib Pipeline with spherical projection ($x, y, z$ on unit sphere), standard scaling, and K-Means ($k=8$, max iterations 30).
7. **Stage 7 — HDFS Export (Person C):** Exports single-part headered CSV summaries to `output/`.
8. **Stage 8 — MongoDB Document Loading (Person C):** Stages and loads 4 collections (`clusters`, `cluster_points`, `region_stats`, `monthly_trends`) with indexes into the `quakes` database.

---

## 6. How to Run

### Prerequisites
- Docker & Docker Compose (or Python 3.10+ virtual environment)
- At least 12 GB RAM allocated to Docker Engine

### Starting the Cluster & Running the Pipeline
```bash
# 1. Start cluster containers
cd config
docker compose up -d

# 2. Initialize cluster daemons
docker exec master bash /opt/project/scripts/init_cluster.sh

# 3. Execute the single-command end-to-end pipeline
docker exec master bash /opt/project/pipeline/run_pipeline.sh
```

### Running Local ML & MongoDB Stages Independently
```bash
# Evaluate k selection
python src/analytics/select_k.py --input output/full_clean.parquet --output output/k_selection.csv --k-min 4 --k-max 12

# Run KMeans clustering
python src/analytics/kmeans_clustering.py --input output/full_clean.parquet --output output/ml --k 8 --mode xyz

# Load MongoDB collections
python src/analytics/mongo_loader.py --mongo-uri mongodb://localhost:27017 --out-dir output/

# Execute Demo Queries
python src/analytics/mongo_queries.py --mongo-uri mongodb://localhost:27017
```

---

## 7. Results & Key Visualizations

### Clustering Evidence & Optimal $k$ ($k=8$)
- **Measured Silhouette Score:** `0.4926` (Full dataset), `0.4807` (20% sample)
- **Tectonic Isolation:** Cluster 3 isolates ultra-deep subduction earthquakes (average depth **510.19 km** in the Tonga-Fiji region). Cluster 7 encapsulates the Western Pacific Ring of Fire containing the maximum recorded earthquake (M8.8).

| Visual Artifact | File Location | Description |
|:---|:---|:---|
| **Cluster Scatter** | `output/figures/cluster_scatter.png` | Global longitude vs. latitude colored by 8 MLlib clusters |
| **$k$-Selection** | `output/figures/k_selection.png` | Elbow curve (WSSSE) and Silhouette score comparison across $k=4$ to 12 |
| **Active Regions** | `output/figures/top_regions.png` | Horizontal bar chart of top 12 seismic regions |
| **Magnitude Distribution** | `output/figures/mag_distribution.png` | Event frequency per magnitude class (log scale) |
| **Yearly & Monthly Trends** | `output/figures/yearly_trend.png` / `monthly_trend.png` | 10-year activity trend with 12-month rolling average |
| **Interactive World Map** | `output/cluster_map.html` | Folium world heatmap with interactive cluster popup markers |

---

## 8. Team Roles & Contributions

| Person | Primary Role | Owned Responsibilities & Modules | Completed Key Deliverables |
|:---|:---|:---|:---|
| **Person A** | **Cluster & Infrastructure Lead** | Docker cluster definition, Hadoop 3-node architecture, HDFS replication, YARN ResourceManager, Hive metastore setup | `config/docker-compose.yml`, `config/hadoop/*.xml`, `scripts/init_cluster.sh`, `docs/cluster-setup.md`, `screenshots/` |
| **Person B** | **Data Engineering & Processing Lead** | PySpark data cleansing, geographic region extraction, depth/magnitude feature enrichment, Hive queries | `src/processing/clean_data.py`, `src/processing/analytics_full.py`, `src/analytics/hive_queries.hql`, `output/*.csv` |
| **Person C** | **ML, Storage & Visualization Lead** | Spark MLlib KMeans pipeline, spherical feature engineering ($xyz$), $k$-selection (elbow/silhouette), MongoDB loader & demo queries, Folium interactive maps, pipeline runner, docs | `src/analytics/kmeans_clustering.py`, `src/analytics/select_k.py`, `src/analytics/mongo_loader.py`, `src/analytics/mongo_queries.py`, `docs/visualization.ipynb`, `docs/cluster-analysis.md`, `pipeline/run_pipeline.sh`, `README.md` |

---

## 9. Repository Structure

```
Global-Earthquake-Analytics/
|-- config/                     # Docker Compose, Dockerfiles, Hadoop/Spark/Hive XML configs
|-- dataset/                    # USGS earthquake catalog source data and downloaders
|-- docs/                       # Project documentation, cluster analysis, demo script, Jupyter notebook
|   |-- cluster-analysis.md     # Geological interpretation of MLlib clusters against tectonic belts
|   |-- cluster-setup.md        # 3-node cluster architecture, daemon specifications, and ports
|   |-- demo-script.md          # 3-minute narration script with demo checklist
|   |-- failover-test.md        # Node failover and HDFS re-replication validation procedures
|   `-- visualization.ipynb     # Reproducible visual exploration notebook
|-- output/                     # Compact CSV aggregates, interactive HTML map, and figures
|   |-- figures/                # Publication-quality PNG figures (scatter, trends, elbow, distributions)
|   |-- cluster_map.html        # Interactive Folium world map with heatmap and cluster popups
|   |-- cluster_summary.csv     # Per-cluster summary metrics and top 3 regions
|   |-- points_sample.csv       # Sampled point-level cluster predictions (20,000 rows)
|   `-- k_selection.csv         # Measured WSSSE and Silhouette scores across candidate k
|-- pipeline/                   # End-to-end automated pipeline orchestration
|   `-- run_pipeline.sh         # Fail-fast bash script executing the complete pipeline
|-- screenshots/                # Cluster health, HDFS FSCK, YARN UI, and terminal run evidence
|-- scripts/                    # Cluster initialization, failover drills, and status verification
`-- src/                        # Source code for ingestion, processing, analytics, and ML
    |-- analytics/              # MLlib clustering, select_k, MongoDB loader and queries
    |-- ingestion/              # HDFS dataset ingestion scripts
    `-- processing/             # PySpark data cleansing and aggregation scripts
```
