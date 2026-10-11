#!/usr/bin/env bash
# Start HiveServer2 in the background (run after init_cluster.sh):
#   docker exec master bash /opt/project/scripts/start_hive.sh
set -e
mkdir -p /data/hive /tmp/hive && cd /data/hive
if [ ! -d /data/hive/metastore_db ]; then
  echo "Initialising Hive metastore (first time only)..."
  schematool -dbType derby -initSchema
fi
if nc -z localhost 10000 2>/dev/null; then echo "HiveServer2 already running"; exit 0; fi

# Hive 3.1.3 assumes Java 8's URLClassLoader. Keep Hadoop/Spark on Java 11.
HIVE_JAVA_HOME=${HIVE_JAVA_HOME:-/opt/java8}
HADOOP_ENV_FILE="${HADOOP_CONF_DIR}/hadoop-env.sh"
HADOOP_ENV_BACKUP=$(mktemp)
cp "$HADOOP_ENV_FILE" "$HADOOP_ENV_BACKUP"
restore_hadoop_env() {
  cp "$HADOOP_ENV_BACKUP" "$HADOOP_ENV_FILE"
  rm -f "$HADOOP_ENV_BACKUP"
}
trap restore_hadoop_env EXIT
sed -i "s#^export JAVA_HOME=.*#export JAVA_HOME=${HIVE_JAVA_HOME}#" "$HADOOP_ENV_FILE"
export JAVA_HOME="$HIVE_JAVA_HOME"

setsid nohup hive --service hiveserver2 > /var/log/hiveserver2.log 2>&1 < /dev/null &
echo "Starting HiveServer2 (takes 30-90 seconds)..."
for i in $(seq 1 60); do
  if nc -z localhost 10000 2>/dev/null; then echo "HiveServer2 is up on port 10000"; exit 0; fi
  sleep 3
done
echo "HiveServer2 did not start. Check: tail -50 /var/log/hiveserver2.log"; exit 1
