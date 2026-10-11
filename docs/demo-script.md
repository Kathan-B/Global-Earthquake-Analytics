# Demo script

Use this as the final 3-minute demonstration outline for the Earthquake Analytics cluster.

## Opening
"This is the 3-node Hadoop cluster for earthquake analytics. The master runs HDFS and YARN, while worker1 and worker2 host DataNodes and NodeManagers. MongoDB is also running on the same docker network."

## Cluster health
- Show `docker compose ps`.
- Show `docker exec master jps`.
- Show `docker exec master hdfs dfsadmin -report`.
- Show `docker exec master yarn node -list`.

## HDFS and data layout
- Show the uploaded dataset in `/earthquake/raw`.
- Show the `fsck` block summary to confirm replication and two DataNodes.
- Point out the 16 MB block sizing and replication factor of 2.

## Spark on YARN
- Start a sample Spark job such as `spark-submit --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100`.
- Highlight the running application in the YARN UI.
- Briefly show the Spark UI showing executors across both workers.

## Hive and MongoDB
- Explain that Hive is started via `start_hive.sh` and listens on port 10000.
- Show that MongoDB is reachable on `localhost:27017`.
- Mention the project uses both HDFS and MongoDB for the analytics pipeline.

## Closing
"This cluster is operational, resilient, and ready for the analytics workload. We can now support B and C on the same shared platform."
