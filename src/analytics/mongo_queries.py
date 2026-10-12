#!/usr/bin/env python3
"""
mongo_queries.py — Four demo queries against MongoDB 'quakes' database.

Person C deliverable.
Queries:
1. Top 5 clusters by average magnitude (avg_mag, top_region_1, quake_count)
2. 10 regions with strongest maximum recorded earthquake (max_mag, quake_count)
3. Average depth and magnitude by cluster, aggregated from sampled cluster_points
4. 3 busiest year/month combinations from monthly_trends

Reads MONGO_URI from env (default: mongodb://localhost:27017 or mongodb://mongo:27017)
"""

import os
import sys
import pprint
import argparse
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


def get_db(uri, db_name="quakes", timeout_ms=5000):
    client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
    client.admin.command("ping")
    return client, client[db_name]


def query_1_top_clusters_by_magnitude(db):
    print("\n" + "=" * 60)
    print("DEMO QUERY 1: Top 5 Clusters by Average Magnitude")
    print("=" * 60)
    col = db["clusters"]
    if col.count_documents({}) == 0:
        print("Note: 'clusters' collection is empty.")
        return

    cursor = col.find(
        {},
        {"_id": 0, "cluster": 1, "avg_mag": 1, "max_mag": 1, "quake_count": 1, "top_region_1": 1, "center_lat": 1, "center_lon": 1}
    ).sort("avg_mag", DESCENDING).limit(5)

    results = list(cursor)
    for i, r in enumerate(results, 1):
        print(f" {i}. Cluster {r.get('cluster')}: Avg Mag = {r.get('avg_mag')}, Max Mag = {r.get('max_mag')}, Count = {r.get('quake_count')}, Primary Region: {r.get('top_region_1')} (Lat: {r.get('center_lat')}, Lon: {r.get('center_lon')})")


def query_2_strongest_regions(db):
    print("\n" + "=" * 60)
    print("DEMO QUERY 2: Top 10 Regions with Strongest Maximum Earthquake")
    print("=" * 60)
    col = db["region_stats"]
    if col.count_documents({}) == 0:
        print("Note: 'region_stats' collection is empty.")
        return

    cursor = col.find(
        {},
        {"_id": 0, "region": 1, "max_mag": 1, "avg_mag": 1, "quake_count": 1}
    ).sort("max_mag", DESCENDING).limit(10)

    results = list(cursor)
    for i, r in enumerate(results, 1):
        print(f" {i:2d}. {r.get('region')}: Max Mag = {r.get('max_mag')}, Avg Mag = {r.get('avg_mag')}, Total Quakes = {r.get('quake_count')}")


def query_3_cluster_points_aggregation(db):
    print("\n" + "=" * 60)
    print("DEMO QUERY 3: Aggregated Average Depth & Magnitude from Sampled Points")
    print("=" * 60)
    col = db["cluster_points"]
    if col.count_documents({}) == 0:
        print("Note: 'cluster_points' collection is empty.")
        return

    pipeline = [
        {
            "$group": {
                "_id": "$cluster",
                "sample_count": {"$sum": 1},
                "calc_avg_depth": {"$avg": "$depth"},
                "calc_avg_mag": {"$avg": "$mag"},
                "calc_max_mag": {"$max": "$mag"},
            }
        },
        {"$sort": {"_id": 1}}
    ]
    results = list(col.aggregate(pipeline))
    for r in results:
        depth_val = f"{r.get('calc_avg_depth'):.2f} km" if r.get('calc_avg_depth') is not None else "N/A"
        mag_val = f"{r.get('calc_avg_mag'):.3f}" if r.get('calc_avg_mag') is not None else "N/A"
        print(f" Cluster {r.get('_id')}: Sample Count = {r.get('sample_count')}, Avg Depth = {depth_val}, Avg Mag = {mag_val}, Max Mag = {r.get('calc_max_mag')}")


def query_4_busiest_periods(db):
    print("\n" + "=" * 60)
    print("DEMO QUERY 4: 3 Busiest Year/Month Periods by Earthquake Frequency")
    print("=" * 60)
    col = db["monthly_trends"]
    if col.count_documents({}) == 0:
        print("Note: 'monthly_trends' collection is empty.")
        return

    # Check which count column exists
    sample = col.find_one({})
    count_key = "quake_count" if "quake_count" in sample else ("count" if "count" in sample else None)

    if count_key:
        cursor = col.find(
            {},
            {"_id": 0, "year": 1, "month": 1, count_key: 1, "avg_mag": 1}
        ).sort(count_key, DESCENDING).limit(3)
        results = list(cursor)
        for i, r in enumerate(results, 1):
            print(f" {i}. Year {r.get('year')}, Month {r.get('month'):02d}: Events = {r.get(count_key)}, Avg Mag = {r.get('avg_mag')}")
    else:
        cursor = col.find({}, {"_id": 0}).limit(3)
        pprint.pprint(list(cursor))


def main():
    parser = argparse.ArgumentParser(description="MongoDB Earthquake Demo Queries")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"), help="MongoDB connection URI")
    parser.add_argument("--db-name", default="quakes", help="Database name")
    args = parser.parse_args()

    client = None
    try:
        client, db = get_db(args.mongo_uri, args.db_name)
        print(f"Connected to MongoDB database '{args.db_name}' at {args.mongo_uri}")
        query_1_top_clusters_by_magnitude(db)
        query_2_strongest_regions(db)
        query_3_cluster_points_aggregation(db)
        query_4_busiest_periods(db)
        print("\nAll demo queries executed successfully.")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"Error: Unable to connect to MongoDB at {args.mongo_uri}: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
