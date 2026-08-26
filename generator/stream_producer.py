"""Writes a small live batch of synthetic order events to the S3 stream-
landing prefix that jobs/streaming/stream_order_events.py polls.

Originally designed for Kinesis (PutRecords) — switched to writing JSON
files directly to S3 after Kinesis hit SubscriptionRequiredException on
this account (see docs/COST.md, docs/RUNBOOK.md). This is deliberately
separate from the 100M-row historical batch dataset: the streaming path's
job is to prove idempotency/watermarking/late-handling on events happening
*now*, not to replay history.

Usage:
    python stream_producer.py --count 500 --late-fraction 0.1 --bucket retail-lakehouse-xxxxx

Each run uses fresh event_ids by default. Pass --replay-file to re-send a
previously-emitted batch verbatim (same event_ids, new filename) — that's
the actual idempotency test: run once, note the curated.fact_order_item row
count; clear the streaming job's checkpoint and restart it; row count must
be unchanged even though every file gets reprocessed from scratch.
"""
import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np


def make_events(n: int, late_fraction: float, seed: int) -> list[dict]:
    rng = np.random.default_rng(seed)
    now = datetime.now(timezone.utc)
    is_late = rng.random(n) < late_fraction

    events = []
    for i in range(n):
        if is_late[i]:
            event_ts = now - timedelta(minutes=float(rng.uniform(20, 90)))
        else:
            event_ts = now - timedelta(seconds=float(rng.uniform(0, 30)))
        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": str(uuid.uuid4()),
            "order_line_id": 1,
            "customer_id": f"devcust_{rng.integers(0, 2000):06d}",
            "product_id": f"B{rng.integers(0, 4_000_000):09d}",
            "seller_id": f"seller_{rng.integers(0, 50_000):07d}",
            "quantity": int(rng.integers(1, 6)),
            "unit_price": round(float(rng.uniform(5, 200)), 2),
            "discount_amount": round(float(rng.choice([0, 0, 5, 10])), 2),
            "order_status": "created",
            # Explicit "Z" suffix — a naive string with no zone marker gets
            # parsed by Spark's TimestampType cast using the SESSION's
            # local timezone, not UTC. Confirmed the hard way: events
            # landed ~5.5 hours "in the past" (IST offset) and every event
            # in a batch registered as late regardless of its intended
            # on-time/late split.
            "event_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return events


def send(events: list[dict], bucket: str) -> None:
    """One JSON-lines file per call, landed under stream_landing/ — Spark's
    file-source streaming reader picks up new files in this prefix."""
    s3 = boto3.client("s3")
    body = "\n".join(json.dumps(e) for e in events).encode("utf-8")
    key = f"stream_landing/order_events/batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"[done] wrote {len(events):,} events to s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--late-fraction", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--save-to", type=str, default=None, help="Save the generated events to a JSON file for later --replay-file use")
    parser.add_argument("--replay-file", type=str, default=None, help="Re-send a previously saved batch verbatim (same event_ids) instead of generating new ones")
    args = parser.parse_args()

    if args.replay_file:
        with open(args.replay_file) as f:
            events = json.load(f)
        print(f"[replay] re-sending {len(events):,} events from {args.replay_file} verbatim")
    else:
        events = make_events(args.count, args.late_fraction, args.seed)
        if args.save_to:
            with open(args.save_to, "w") as f:
                json.dump(events, f)
            print(f"[saved] {args.save_to} (for a later --replay-file idempotency test)")

    send(events, args.bucket)


if __name__ == "__main__":
    main()
