#!/usr/bin/env bash
# Usage (inside master): bash /opt/project/src/ingestion/load_to_hdfs.sh [path-to-csv]
set -euo pipefail

SRC=${1:-/opt/project/dataset/quakes.csv}
DEST=/earthquake/raw

hdfs dfs -mkdir -p "$DEST"
hdfs dfs -D dfs.blocksize=16777216 -D dfs.replication=2 -put -f "$SRC" "$DEST/quakes.csv"

echo "---- listing ----"
hdfs dfs -ls -h "$DEST"
echo "---- first 3 lines ----"
hdfs dfs -cat "$DEST/quakes.csv" | head -3
echo "---- block layout ----"
hdfs fsck "$DEST/quakes.csv" -files -blocks -locations
