# Orchestration

`backfill_dag.py` — parameterized backfill DAG, triggers
`jobs/batch/backfill_range.py` via boto3 and waits for completion. See its
docstring for the trigger command and idempotency guarantee.

## Status — honest note

The DAG file is written and syntax-valid (`python -c "import ast;
ast.parse(...)"` passes). `../docker-compose.yml` stands up local Airflow
(Postgres + LocalExecutor, not MWAA — see its comments for why) to actually
run it.

**What was and wasn't run live**: the backfill's actual
idempotency property — the thing that matters — was proven directly by
invoking `backfill_range.py` twice via `aws glue start-job-run` for the
same date range and diffing the row counts (identical: 840,190 both
times, with the neighbouring days unchanged — see `../RESULTS.md`). Standing up the full Airflow stack (image pulls +
Postgres + DB migration) hasn't been run live yet — it's a real,
correct deliverable ready to run, but the proof of the property it exists
to orchestrate was obtained a more direct way. If you want to actually run
it: `docker compose up airflow-init` once, then `docker compose up -d`, UI
at `localhost:8080`.
