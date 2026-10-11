#!/usr/bin/env bash
# Evidence for the demo (run inside master):
#   docker exec master bash /opt/project/scripts/cluster_status.sh
echo "=================== HDFS: live DataNodes ==================="
hdfs dfsadmin -report | grep -E "Live datanodes|^Name:|^Hostname:|Configured Capacity|DFS Used:"
echo
echo "=================== YARN: worker nodes ====================="
yarn node -list
echo
echo "=================== HDFS: block distribution ==============="
hdfs fsck /earthquake/raw -files -blocks -locations | tail -25
echo
echo "=================== Config in use =========================="
hdfs getconf -confKey dfs.replication
hdfs getconf -confKey dfs.blocksize
hdfs getconf -confKey fs.defaultFS
