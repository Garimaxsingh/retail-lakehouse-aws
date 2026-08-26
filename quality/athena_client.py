"""Thin Athena helper — every DQ check runs as a query, not a Spark job.
Checks are lightweight aggregates; Athena's per-query cost ($5/TB scanned,
seconds of latency) beats spinning up a Glue job 60+ times over for this.
"""
import time

import boto3
import pandas as pd

WORKGROUP = "retail-lakehouse-wg"


def run_query(sql: str, database: str, region: str = "ap-south-1") -> pd.DataFrame:
    client = boto3.client("athena", region_name=region)
    qid = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status.get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena query failed ({state}): {reason}\nSQL: {sql}")

    rows = []
    paginator = client.get_paginator("get_query_results")
    columns = None
    for page in paginator.paginate(QueryExecutionId=qid):
        for i, row in enumerate(page["ResultSet"]["Rows"]):
            values = [c.get("VarCharValue") for c in row["Data"]]
            if columns is None:
                columns = values
                continue
            rows.append(values)
    return pd.DataFrame(rows, columns=columns)
