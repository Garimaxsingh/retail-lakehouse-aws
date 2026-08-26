"""Re-pushes CloudWatch metrics from an already-computed dq_run_log.csv —
avoids re-running 488 Athena queries just to fix how metrics are shaped.
Pushed in run_date order so the alarm's time series reads left-to-right
the way the simulated runs actually happened.
"""
import csv
import time
from pathlib import Path

import boto3

NAMESPACE = "RetailLakehouse/DataQuality"
LOG = Path(__file__).parent / "results" / "dq_run_log.csv"


def main():
    cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")
    with open(LOG, newline="") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r["run_date"])

    pushed = 0
    for row in rows:
        if row["passed"] not in ("True", "False"):
            continue
        cloudwatch.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[{
                "MetricName": row["check"],
                "Value": 1.0 if row["passed"] == "True" else 0.0,
                "Unit": "None",
            }],
        )
        pushed += 1
        if pushed % 50 == 0:
            print(f"[push] {pushed} metrics pushed...", flush=True)
            time.sleep(0.2)  # stay well under API rate limits

    print(f"[done] {pushed} metric datapoints pushed to CloudWatch namespace {NAMESPACE}", flush=True)


if __name__ == "__main__":
    main()
