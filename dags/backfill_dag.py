"""Parameterized backfill DAG — triggers jobs/batch/backfill_range.py for a
date range, waits for completion, and fails loudly (not silently) if the
Glue job fails.

Runs in local Docker Airflow, invoking AWS via boto3 — not MWAA (~$350/mo,
would eat the whole Free Plan credit budget in under three weeks, per the
project's own execution plan).

Trigger a backfill with:
    airflow dags trigger retail_lakehouse_backfill \
        --conf '{"start_date": "2024-06-01", "end_date": "2024-06-07"}'

Safe to re-run: backfill_range.py uses Iceberg's overwritePartitions(),
which only replaces the date partitions actually reprocessed — running the
same range twice produces identical row counts (see RESULTS.md for the
executed proof), not compounding duplicates.
"""
from datetime import datetime

import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param

JOB_NAME = "retail-lakehouse-backfill-range"
REGION = "ap-south-1"


@dag(
    dag_id="retail_lakehouse_backfill",
    schedule=None,  # triggered manually / on-demand, not a recurring schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "start_date": Param("2024-01-01", type="string", format="date"),
        "end_date": Param("2024-01-07", type="string", format="date"),
    },
    tags=["backfill", "batch"],
)
def retail_lakehouse_backfill():

    @task
    def run_backfill(**context) -> str:
        params = context["params"]
        start_date, end_date = params["start_date"], params["end_date"]

        client = boto3.client("glue", region_name=REGION)
        run_id = client.start_job_run(
            JobName=JOB_NAME,
            Arguments={"--start_date": start_date, "--end_date": end_date},
        )["JobRunId"]
        print(f"Started {JOB_NAME} run {run_id} for {start_date}..{end_date}")

        waiter_states = {"SUCCEEDED", "FAILED", "ERROR", "STOPPED", "TIMEOUT"}
        state = None
        while state not in waiter_states:
            import time
            time.sleep(30)
            run = client.get_job_run(JobName=JOB_NAME, RunId=run_id)["JobRun"]
            state = run["JobRunState"]

        if state != "SUCCEEDED":
            error = run.get("ErrorMessage", "no error message")
            raise AirflowException(f"Backfill job {run_id} ended in {state}: {error}")

        print(f"Backfill {run_id} succeeded for {start_date}..{end_date}")
        return run_id

    run_backfill()


retail_lakehouse_backfill()
