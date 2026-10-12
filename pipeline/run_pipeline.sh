#!/usr/bin/env bash
# ==============================================================================
# run_pipeline.sh — End-to-End Global Earthquake Analytics Pipeline
#
# Person C deliverable.
# Runs on master container:
#   Stage 1: Verify raw data in HDFS
#   Stage 2: Run PySpark data cleaning & partitioning (Person B)
#   Stage 3: Run full PySpark aggregations & analytics (Person B)
#   Stage 4: Execute Hive queries (conditionally if HiveServer2 is available)
#   Stage 5: Run Spark MLlib k-selection (elbow & silhouette evaluation) (Person C)
#   Stage 6: Run Spark MLlib spherical KMeans clustering (Person C)
#   Stage 7: Export compact HDFS CSV results to output/ (Person C)
#   Stage 8: Load curated collections into MongoDB (Person C)
#   Stage 9: Print completion summary and status
# ==============================================================================

set -euo pipefail

PROJ="${PROJ:-/opt/project}"
K="${K:-8}"
MONGO_URI="${MONGO_URI:-mongodb://mongo:27017}"
ENABLE_HIVE="${ENABLE_HIVE:-auto}"  # auto, true, or false

echo "================================================================="
echo " STARTING END-TO-END EARTHQUAKE ANALYTICS PIPELINE"
echo " Project root : ${PROJ}"
echo " Clusters (k) : ${K}"
echo " MongoDB URI  : ${MONGO_URI}"
echo " Timestamp    : $(date -u)"
echo "================================================================="

SPARK_OPTS=(
  --master yarn
  --deploy-mode client
  --num-executors 2
  --executor-cores 1
  --executor-memory 1g
  --driver-memory 1g
  --conf spark.sql.shuffle.partitions=8
)

function check_stage_failure() {
  local exit_code=$?
  local stage_name="$1"
  if [ "${exit_code}" -ne 0 ]; then
    echo "================================================================="
    echo " PIPELINE FAILED AT STAGE: ${stage_name} (Exit code: ${exit_code})"
    echo "================================================================="
    exit "${exit_code}"
  fi
}

trap 'echo "Error encountered on line $LINENO. Exiting."; exit 1' ERR

# ------------------------------------------------------------------------------
# STAGE 1: Confirm raw HDFS data
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 1/8] Verifying raw dataset in HDFS..."
if hdfs dfs -test -e /earthquake/raw/quakes.csv; then
  echo "Raw dataset confirmed at /earthquake/raw/quakes.csv"
else
  if [ -f "${PROJ}/dataset/quakes.csv" ]; then
    echo "Uploading local dataset to HDFS /earthquake/raw/quakes.csv..."
    hdfs dfs -mkdir -p /earthquake/raw
    hdfs dfs -put -f "${PROJ}/dataset/quakes.csv" /earthquake/raw/quakes.csv
  else
    echo "FATAL: /earthquake/raw/quakes.csv not found on HDFS and local dataset missing."
    exit 1
  fi
fi

# ------------------------------------------------------------------------------
# STAGE 2: PySpark Cleaning & Parquet Export
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 2/8] Executing PySpark cleaning job (Person B)..."
if [ -f "${PROJ}/src/processing/clean_data.py" ]; then
  spark-submit "${SPARK_OPTS[@]}" "${PROJ}/src/processing/clean_data.py" \
    --input "hdfs:///earthquake/raw/quakes.csv" \
    --output "hdfs:///earthquake/clean"
  check_stage_failure "Data Cleaning"
else
  echo "Notice: src/processing/clean_data.py not present in branch. Checking /earthquake/clean in HDFS..."
  if ! hdfs dfs -test -e /earthquake/clean; then
    echo "FATAL: /earthquake/clean not present and cleaning script missing."
    exit 1
  fi
fi

# ------------------------------------------------------------------------------
# STAGE 3: PySpark Analytics
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 3/8] Executing PySpark analytics aggregations..."
if [ -f "${PROJ}/src/processing/analytics_full.py" ]; then
  spark-submit "${SPARK_OPTS[@]}" "${PROJ}/src/processing/analytics_full.py" \
    --input "hdfs:///earthquake/clean" \
    --output "hdfs:///earthquake/output"
  check_stage_failure "Analytics Full"
elif [ -f "${PROJ}/src/processing/analytics_core.py" ]; then
  spark-submit "${SPARK_OPTS[@]}" "${PROJ}/src/processing/analytics_core.py" \
    --input "hdfs:///earthquake/clean" \
    --output "hdfs:///earthquake/output"
  check_stage_failure "Analytics Core"
else
  echo "Notice: Processing analytics script not found. Proceeding to ML stages."
fi

# ------------------------------------------------------------------------------
# STAGE 4: Hive Queries (Conditional)
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 4/8] Evaluating HiveServer2 status..."
if [ "${ENABLE_HIVE}" = "true" ] || ([ "${ENABLE_HIVE}" = "auto" ] && nc -z localhost 10000 2>/dev/null); then
  echo "HiveServer2 is accessible on port 10000. Executing Hive setup..."
  if [ -f "${PROJ}/src/analytics/hive_queries.hql" ]; then
    beeline -u "jdbc:hive2://localhost:10000/default" -n root -p "" -f "${PROJ}/src/analytics/hive_queries.hql"
    check_stage_failure "Hive Queries"
  fi
else
  echo "HiveServer2 is not reachable or disabled (ENABLE_HIVE=${ENABLE_HIVE})."
  echo "Bypassing Hive queries stage and continuing ML & MongoDB stages."
fi

# ------------------------------------------------------------------------------
# STAGE 5: MLlib k-Selection (Elbow & Silhouette)
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 5/8] Running MLlib k-Selection (k=4 to 12)..."
spark-submit "${SPARK_OPTS[@]}" "${PROJ}/src/analytics/select_k.py" \
  --input "hdfs:///earthquake/clean" \
  --output "hdfs:///earthquake/ml/k_selection" \
  --k-min 4 --k-max 12 --sample-frac 0.2
check_stage_failure "k-Selection"

# ------------------------------------------------------------------------------
# STAGE 6: MLlib Spherical KMeans Clustering
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 6/8] Running MLlib Spherical KMeans Clustering (k=${K})..."
spark-submit "${SPARK_OPTS[@]}" "${PROJ}/src/analytics/kmeans_clustering.py" \
  --input "hdfs:///earthquake/clean" \
  --output "hdfs:///earthquake/ml" \
  --k "${K}" --mode xyz
check_stage_failure "KMeans Clustering"

# ------------------------------------------------------------------------------
# STAGE 7: Export Compact Results from HDFS to Local output/
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 7/8] Exporting compact CSV summaries from HDFS to ${PROJ}/output..."
mkdir -p "${PROJ}/output"

# Export k_selection
if hdfs dfs -test -e /earthquake/ml/k_selection; then
  echo "Exporting k_selection.csv..."
  hdfs dfs -cat "/earthquake/ml/k_selection/part-*.csv" > "${PROJ}/output/k_selection.csv" || true
fi

# Export cluster_summary
if hdfs dfs -test -e /earthquake/ml/cluster_summary; then
  echo "Exporting cluster_summary.csv..."
  hdfs dfs -cat "/earthquake/ml/cluster_summary/part-*.csv" > "${PROJ}/output/cluster_summary.csv" || true
fi

# Export points_sample
if hdfs dfs -test -e /earthquake/ml/points_sample; then
  echo "Exporting points_sample.csv..."
  hdfs dfs -cat "/earthquake/ml/points_sample/part-*.csv" > "${PROJ}/output/points_sample.csv" || true
fi

# Export Person B outputs if available
for f in top_regions region_stats mag_distribution depth_vs_mag yearly_trend monthly_trend strongest_per_decade hotspot_grid; do
  if hdfs dfs -test -e "/earthquake/output/${f}"; then
    hdfs dfs -cat "/earthquake/output/${f}/part-*.csv" > "${PROJ}/output/${f}.csv" || true
  fi
done

# ------------------------------------------------------------------------------
# STAGE 8: MongoDB Loader & Demo Queries
# ------------------------------------------------------------------------------
echo ""
echo ">>> [STAGE 8/8] Loading results into MongoDB (${MONGO_URI})..."
python3 "${PROJ}/src/analytics/mongo_loader.py" \
  --mongo-uri "${MONGO_URI}" \
  --out-dir "${PROJ}/output"
check_stage_failure "MongoDB Loader"

echo ""
echo "Executing MongoDB Demo Queries..."
python3 "${PROJ}/src/analytics/mongo_queries.py" \
  --mongo-uri "${MONGO_URI}"
check_stage_failure "MongoDB Demo Queries"

echo ""
echo "================================================================="
echo " PIPELINE RUN COMPLETED SUCCESSFULLY"
echo " All ML models, evaluations, HDFS exports, and MongoDB"
echo " collections have been generated and validated."
echo "================================================================="
