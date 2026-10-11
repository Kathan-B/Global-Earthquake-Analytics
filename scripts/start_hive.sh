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
setsid nohup hive --service hiveserver2 > /var/log/hiveserver2.log 2>&1 < /dev/null &
echo "Starting HiveServer2 (takes 30-90 seconds)..."
for i in $(seq 1 60); do
  if nc -z localhost 10000 2>/dev/null; then echo "HiveServer2 is up on port 10000"; exit 0; fi
  sleep 3
done
echo "HiveServer2 did not start. Check: tail -50 /var/log/hiveserver2.log"; exit 1
