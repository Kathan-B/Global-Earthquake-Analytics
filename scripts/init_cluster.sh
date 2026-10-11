#!/usr/bin/env bash
# Run ONCE after the first start (safe to re-run):
#   docker exec master bash /opt/project/scripts/init_cluster.sh
set -e
echo "Waiting for 2 live DataNodes..."
until [ "$(hdfs dfsadmin -report 2>/dev/null | grep -c '^Name:')" -ge 2 ]; do sleep 3; done
hdfs dfsadmin -safemode wait

hdfs dfs -mkdir -p /earthquake/raw /earthquake/clean /earthquake/output /earthquake/ml \
                   /tmp /user/hive/warehouse /spark-jars
hdfs dfs -chmod -R 777 /tmp /user/hive /earthquake

if ! hdfs dfs -test -e /spark-jars/_done; then
  echo "Uploading Spark jars to HDFS (one time, 1-3 minutes)..."
  hdfs dfs -put -f /opt/spark/jars/*.jar /spark-jars/
  hdfs dfs -touchz /spark-jars/_done
fi
echo "INIT DONE. Try: hdfs dfsadmin -report | head -20"
