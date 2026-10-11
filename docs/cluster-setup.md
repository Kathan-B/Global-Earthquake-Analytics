# Cluster setup (Person A)

Cluster: 3 nodes as Docker containers on one host (1 master, 2 workers) + MongoDB.

| Node | Hostname | Daemons |
|------|----------|---------|
| Master | master | HDFS NameNode, YARN ResourceManager, Spark driver (client mode), HiveServer2 |
| Worker 1 | worker1 | HDFS DataNode, YARN NodeManager (Spark executor) |
| Worker 2 | worker2 | HDFS DataNode, YARN NodeManager (Spark executor) |
| Mongo | mongo | MongoDB 7 |

Versions: Hadoop 3.3.6, Spark 3.5.1, Hive 3.1.3, Java 11. HDFS replication 2, block size 16 MB.

## Start
```
cd config
docker compose build          # first time only, 15-25 min (downloads ~1.5 GB)
docker compose up -d
docker exec master bash /opt/project/scripts/init_cluster.sh
docker exec master bash /opt/project/scripts/start_hive.sh
```

## Evidence for the demo
```
docker exec master jps            # NameNode, ResourceManager
docker exec worker1 jps           # DataNode, NodeManager
docker exec worker2 jps
docker exec master bash /opt/project/scripts/cluster_status.sh
```
UIs: http://localhost:9870 (HDFS), http://localhost:8088 (YARN), http://localhost:4040 (Spark job UI while running).
