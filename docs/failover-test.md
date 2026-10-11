# Failover test

This document captures the required worker-loss validation for the cluster.

## Goal
Verify that the HDFS replication and YARN job scheduling remain healthy when one worker goes down.

## Procedure
1. Confirm the cluster is healthy: `docker exec master jps` and `docker exec master yarn node -list`.
2. Load a dataset into HDFS and ensure replication is 2.
3. Stop one worker container: `docker stop worker1`.
4. Re-run the status and reporting checks:
   - `docker exec master hdfs dfsadmin -report`
   - `docker exec master yarn node -list`
5. Confirm the cluster reports 1 live DataNode and that under-replicated blocks are corrected after recovery.
6. Restart the worker: `docker start worker1`.
7. Wait for the DataNode to come back and re-check HDFS status.

## Expected evidence
- HDFS remains available during the worker outage.
- The `fsck` output shows the cluster recovers without losing the dataset.
- YARN can still schedule work unless the worker was the only host for a task.

## Screen captures to keep
- Healthy cluster before failover.
- `hdfs dfsadmin -report` after stopping a worker.
- `yarn node -list` showing reduced capacity.
- Final recovery state after restarting the worker.
