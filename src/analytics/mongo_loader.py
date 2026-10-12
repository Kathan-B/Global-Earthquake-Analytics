#!/usr/bin/env python3
"""
mongo_loader.py — Safely load earthquake analytics and cluster outputs into MongoDB.

Person C deliverable.
Loads 4 compact datasets into the 'quakes' MongoDB database:
1. clusters        <- cluster_summary.csv   (Index: cluster)
2. cluster_points  <- points_sample.csv     (Index: cluster)
3. region_stats    <- region_stats.csv      (Index: region)
4. monthly_trends  <- monthly_trend.csv     (Index: year, month)

Reads MONGO_URI from env (default: mongodb://localhost:27017 or in docker mongodb://mongo:27017)
Reads OUT_DIR from env (default: output)
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


def get_mongo_client(uri, timeout_ms=5000):
    """Initializes and tests MongoClient connection."""
    client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
    client.admin.command("ping")
    return client


def sanitize_dataframe(df):
    """Converts numpy/pandas types to standard Python types for MongoDB BSON serialization."""
    # Replace NaN, inf, -inf with None
    df = df.replace([np.inf, -np.negative(np.inf)], np.nan)
    records = df.to_dict(orient="records")
    clean_records = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            if pd.isna(v):
                clean_row[k] = None
            elif isinstance(v, (np.integer, int)):
                clean_row[k] = int(v)
            elif isinstance(v, (np.floating, float)):
                clean_row[k] = float(v)
            else:
                clean_row[k] = str(v)
        clean_records.append(clean_row)
    return clean_records


def load_dataset(db, collection_name, csv_path, index_fields, required_fields=None):
    """
    Safely loads a CSV into a MongoDB collection with atomic drop-and-replace and indexing.
    """
    if not os.path.isfile(csv_path):
        print(f"SKIP (missing file): {csv_path} for collection '{collection_name}'")
        return False, "missing"

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"ERROR: Failed to read {csv_path}: {e}", file=sys.stderr)
        return False, "read_error"

    if df.empty:
        print(f"SKIP (empty file): {csv_path} for collection '{collection_name}'")
        return False, "empty"

    if required_fields:
        missing_req = [f for f in required_fields if f not in df.columns]
        if missing_req:
            print(f"ERROR: {csv_path} is missing required fields {missing_req}", file=sys.stderr)
            return False, "schema_error"

    records = sanitize_dataframe(df)

    # Use stage collection for zero-downtime safe replacement
    stage_name = f"{collection_name}_stage"
    stage_col = db[stage_name]
    stage_col.drop()

    if records:
        stage_col.insert_many(records)

    # Create index on staging collection
    if isinstance(index_fields, list):
        index_keys = [(f, ASCENDING) for f in index_fields]
        stage_col.create_index(index_keys)
    elif isinstance(index_fields, str):
        stage_col.create_index([(index_fields, ASCENDING)])

    # Swap stage to production collection atomically
    stage_col.rename(collection_name, dropTarget=True)

    doc_count = db[collection_name].count_documents({})
    print(f"SUCCESS: Loaded {doc_count} documents into collection '{collection_name}' (source: {csv_path})")
    return True, "loaded"


def main():
    parser = argparse.ArgumentParser(description="MongoDB Loader for Earthquake Analytics")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"), help="MongoDB connection URI")
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "output"), help="Output directory containing result CSVs")
    parser.add_argument("--db-name", default="quakes", help="MongoDB target database name")

    args = parser.parse_args()

    print(f"Connecting to MongoDB at: {args.mongo_uri} (database: {args.db_name})...")
    client = None
    try:
        client = get_mongo_client(args.mongo_uri)
        db = client[args.db_name]
        print("Connected to MongoDB successfully.")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"FATAL: Unable to connect to MongoDB at {args.mongo_uri}: {e}", file=sys.stderr)
        sys.exit(1)

    load_targets = [
        {
            "collection": "clusters",
            "csv": os.path.join(args.out_dir, "cluster_summary.csv"),
            "index": "cluster",
            "required": ["cluster", "quake_count", "center_lat", "center_lon"],
        },
        {
            "collection": "cluster_points",
            "csv": os.path.join(args.out_dir, "points_sample.csv"),
            "index": "cluster",
            "required": ["latitude", "longitude", "cluster"],
        },
        {
            "collection": "region_stats",
            "csv": os.path.join(args.out_dir, "region_stats.csv"),
            "index": "region",
            "required": ["region"],
        },
        {
            "collection": "monthly_trends",
            "csv": os.path.join(args.out_dir, "monthly_trend.csv"),
            "index": ["year", "month"],
            "required": ["year", "month"],
        },
    ]

    summary = {"loaded": [], "skipped": [], "failed": []}

    try:
        for target in load_targets:
            col_name = target["collection"]
            csv_path = target["csv"]
            idx = target["index"]
            req = target.get("required")

            success, status = load_dataset(db, col_name, csv_path, idx, req)
            if success:
                summary["loaded"].append(col_name)
            elif status in ("missing", "empty"):
                summary["skipped"].append((col_name, status))
            else:
                summary["failed"].append((col_name, status))

        print("\n--- MongoDB Loader Summary ---")
        print(f"Loaded collections  ({len(summary['loaded'])}): {summary['loaded']}")
        print(f"Skipped collections ({len(summary['skipped'])}): {summary['skipped']}")
        print(f"Failed collections  ({len(summary['failed'])}): {summary['failed']}")

        if summary["failed"]:
            print(f"Completed with errors in collections: {summary['failed']}", file=sys.stderr)
            sys.exit(1)
        print("MongoDB loading stage completed cleanly.")

    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
