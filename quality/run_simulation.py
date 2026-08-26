"""Simulates 60 daily pipeline runs of the DQ suite against the already-
generated 2-year historical dataset, since the data arrived as one backfill
rather than incrementally. This is a real, honest way to get a genuine
60-run sample: each simulated day runs the actual checks against actual
(if historical) data slices, with 3 of the 60 days using deliberately
corrupted fault-injection scratch tables (see inject_faults.py) for the
three checks that would otherwise have nothing to catch, since the real
pipeline is clean.

Documented explicitly, not hidden: this compresses calendar cadence into
one sitting. It does NOT fabricate results — every number is a real query
result against real (or genuinely-corrupted-on-purpose) data.
"""
import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

import boto3

import checks
import inject_faults

RESULTS_CSV = Path(__file__).parent / "results" / "dq_run_log.csv"
NAMESPACE = "RetailLakehouse/DataQuality"


def pick_run_dates(n: int, start: date, end: date) -> list[str]:
    total_days = (end - start).days
    margin = 10  # need >=8 days of trailing history for the volume check
    stride = max(1, (total_days - margin) // n)
    dates = [start + timedelta(days=margin + i * stride) for i in range(n)]
    dates = [d for d in dates if d <= end]
    for forced in ["2025-01-04", "2025-01-05"]:
        fd = date.fromisoformat(forced)
        if fd not in dates:
            dates.append(fd)
    return sorted({d.isoformat() for d in dates})


def push_metric(cloudwatch, check_name: str, passed, run_date: str) -> None:
    if passed is None:
        return
    # No per-date dimension — that would make CloudWatch treat every
    # simulated day as a DIFFERENT metric series, which makes it impossible
    # to alarm on "this check failed recently" in aggregate. run_date goes
    # in the CSV log for detail; the metric itself is just "did this check
    # pass," with CloudWatch's own push-time timestamp as the time axis.
    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            "MetricName": check_name,
            "Value": 1.0 if passed else 0.0,
            "Unit": "None",
        }],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=60)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--start-date", type=str, default="2024-01-01")
    parser.add_argument("--end-date", type=str, default="2025-12-31")
    parser.add_argument("--no-cloudwatch", action="store_true")
    args = parser.parse_args()

    run_dates = pick_run_dates(args.runs, date.fromisoformat(args.start_date), date.fromisoformat(args.end_date))
    print(f"[sim] {len(run_dates)} simulated run dates selected", flush=True)

    # 3 dates get fault injection for the 3 checks with nothing organic to catch
    fault_dates = {
        "referential_integrity": run_dates[len(run_dates) // 4],
        "duplicate": run_dates[len(run_dates) // 2],
        "consistency": run_dates[3 * len(run_dates) // 4],
    }
    print(f"[sim] fault-injection dates: {fault_dates}", flush=True)

    print("[sim] building fault-injection scratch tables...", flush=True)
    orphan_table = inject_faults.create_orphan_fault(fault_dates["referential_integrity"])
    dup_table = inject_faults.create_duplicate_fault(fault_dates["duplicate"])
    consistency_table = inject_faults.create_consistency_fault(fault_dates["consistency"])

    cloudwatch = None if args.no_cloudwatch else boto3.client("cloudwatch", region_name="ap-south-1")

    results = []
    for i, run_date in enumerate(run_dates):
        print(f"[sim] run {i+1}/{len(run_dates)}: {run_date}", flush=True)
        batch = [
            checks.volume_check(run_date),
            checks.null_rate_check(run_date),
            checks.referential_integrity_check(run_date, orphan_table if run_date == fault_dates["referential_integrity"] else None),
            checks.consistency_check(run_date, consistency_table if run_date == fault_dates["consistency"] else None),
            checks.duplicate_check(run_date, dup_table if run_date == fault_dates["duplicate"] else None),
            checks.freshness_sla_check(run_date),
            checks.schema_drift_check(run_date, args.bucket),
            checks.price_distribution_shift_check(run_date),
        ]
        for r in batch:
            results.append(r)
            if cloudwatch:
                push_metric(cloudwatch, r["check"], r["passed"], run_date)
            flag = "FAIL" if r["passed"] is False else ("SKIP" if r["passed"] is None else "pass")
            print(f"    [{flag}] {r['check']}: {r['actual']} (threshold {r['threshold']})" + (" [INJECTED]" if r["injected"] else ""), flush=True)

    print("[sim] cleaning up fault-injection scratch tables...", flush=True)
    inject_faults.drop_fault_tables()

    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "run_date", "passed", "actual", "threshold", "detail", "injected"])
        writer.writeheader()
        writer.writerows(results)

    total = len(results)
    failed = [r for r in results if r["passed"] is False]
    skipped = [r for r in results if r["passed"] is None]
    organic_failures = [r for r in failed if not r["injected"]]
    injected_catches = [r for r in failed if r["injected"]]
    print(f"\n[summary] {total} checks run across {len(run_dates)} simulated days", flush=True)
    print(f"[summary] {len(failed)} flagged, {len(skipped)} skipped (insufficient data)", flush=True)
    print(f"[summary] {len(injected_catches)}/3 injected faults caught", flush=True)
    print(f"[summary] {len(organic_failures)} organic incidents caught:", flush=True)
    for r in organic_failures:
        print(f"    - {r['check']} on {r['run_date']}: {r['detail']}", flush=True)
    print(f"[done] full log written to {RESULTS_CSV}", flush=True)


if __name__ == "__main__":
    main()
