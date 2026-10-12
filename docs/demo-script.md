# 3-Minute Live Demonstration Script: Global Earthquake Analytics

**Project:** Global Earthquake Analytics & Seismic Pattern Detection  
**Team:** 3-Node Hadoop/Spark Cluster, PySpark & MLlib Pipeline, MongoDB, Visual Analytics  
**Presenter:** Person C (Narrating Machine Learning, Storage & Interactive Visuals)  
**Time Limit:** 3 minutes

---

## Stage-by-Stage Narration & Visual Evidence

```
+---------------------------------------------------------------------------------------------------------+
| TIME      | DEMONSTRATION ACTION                        | WHAT TO NARRATE                                |
+---------------------------------------------------------------------------------------------------------+
| 0:00-0:45 | • Display terminal: HDFS storage inspection | "Welcome. To analyze 289,695 global earthquake |
| (Person A |   `hdfs dfs -ls /earthquake/raw`            |  records from the USGS catalog, our 3-node     |
|  & B)     | • Show YARN UI & live DataNodes             |  Hadoop cluster runs HDFS and YARN across      |
|           | • Show PySpark clean partitioned Parquet    |  master, worker1, and worker2 with 16 MB block |
|           |   in `/earthquake/clean`                    |  replication. Person B's PySpark job has       |
|           |                                             |  cleansed missing coordinates and partitioned  |
|           |                                             |  the dataset by year."                         |
+-----------+---------------------------------------------+------------------------------------------------+
| 0:45-1:30 | • Display terminal:                         | "Now entering Person C's MLlib pipeline.       |
| (Person C)|   `spark-submit src/analytics/select_k.py`  |  Rather than naive 2D coordinates which distort|
|           | • Display `output/figures/k_selection.png`  |  at poles and the antimeridian, we project lat |
|           | • Highlight WSSSE elbow & silhouette curve  |  and lon onto a 3D unit sphere: x, y, and z,   |
|           |                                             |  combined with standardized depth and magnitude|
|           |                                             |  Using select_k.py across k=4 to 12 on YARN,   |
|           |                                             |  we evaluated the elbow method and silhouette  |
|           |                                             |  scores. k=8 was selected as the optimal       |
|           |                                             |  balance of variance explained and geological  |
|           |                                             |  coherence, yielding a silhouette of 0.4926."  |
+-----------+---------------------------------------------+------------------------------------------------+
| 1:30-2:15 | • Display terminal:                         | "Here we execute kmeans_clustering.py on the   |
| (Person C)|   `src/analytics/kmeans_clustering.py`      |  clean dataset. The pipeline writes predicted  |
|           | • Show `output/cluster_summary.csv`         |  points to HDFS, writes a 20,000-point sample, |
|           | • Open browser / iframe:                    |  and exports cluster statistics.               |
|           |   `output/cluster_map.html`                 |  On our interactive Folium map, we see real    |
|           | • Click Cluster 3 (Fiji, 510 km depth)      |  tectonic separation: Cluster 3 isolates the   |
|           | • Click Cluster 7 (Ring of Fire, max M8.8)  |  ultra-deep Tonga-Fiji subduction zone at 510  |
|           |                                             |  km average depth. Cluster 7 captures the      |
|           |                                             |  Western Pacific Ring of Fire where the M8.8   |
|           |                                             |  megathrust occurred."                         |
+-----------+---------------------------------------------+------------------------------------------------+
| 2:15-2:45 | • Run terminal command:                     | "For serving and fast query access,            |
| (Person C)|   `python src/analytics/mongo_loader.py`    |  mongo_loader.py stages and populates four     |
|           | • Run `python src/analytics/mongo_queries.py|  curated collections in MongoDB quakes database|
|           | • Highlight Query 1 (Top clusters by mag)   |  with secondary indexes. Running our demo      |
|           | • Highlight Query 3 (Depth aggregation)     |  queries instantly surfaces the top magnitude  |
|           |                                             |  clusters and high-risk seismic hotspots."     |
+-----------+---------------------------------------------+------------------------------------------------+
| 2:45-3:00 | • Display `pipeline/run_pipeline.sh`        | "Finally, our end-to-end orchestration script, |
| (Person C)| • Point to execution logs                   |  run_pipeline.sh, ties the complete workflow   |
| & Summary |                                             |  together from ingestion to MongoDB load in one|
|           |                                             |  command. The system provides a resilient,     |
|           |                                             |  distributed analytical foundation for global  |
|           |                                             |  seismic pattern analysis. Thank you."         |
+---------------------------------------------------------------------------------------------------------+
```

---

## Demo Checklist for Presenters

- [x] Docker containers `master`, `worker1`, `worker2`, `mongo` active.
- [x] Verified HDFS path `/earthquake/clean` contains year-partitioned Parquet.
- [x] `output/figures/k_selection.png` and `cluster_scatter.png` generated and ready to display.
- [x] Interactive map `output/cluster_map.html` opens smoothly in browser.
- [x] `mongo_queries.py` produces readable formatted output for all 4 demo queries.
- [x] Clarified to audience that clustering detects physical historical activity concentrations, not predictive temporal forecasts.
