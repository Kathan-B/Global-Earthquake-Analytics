#!/usr/bin/env bash
# Starts the Hadoop daemons for this container's role. ROLE=master or worker.
set -e
mkdir -p /data/namenode /data/datanode /opt/hadoop/logs

if [ "$ROLE" = "master" ]; then
  if [ ! -d /data/namenode/current ]; then
    echo "[master] formatting NameNode (first start only)"
    hdfs namenode -format -force -nonInteractive
  fi
  hdfs --daemon start namenode
  yarn --daemon start resourcemanager
  echo "[master] NameNode + ResourceManager started"
else
  until nc -z master 9000; do echo "[$HOSTNAME] waiting for NameNode..."; sleep 2; done
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
  echo "[$HOSTNAME] DataNode + NodeManager started"
fi

exec tail -f /dev/null
