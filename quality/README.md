# Data Quality Suite

Eight checks from the project spec's DQ table, orchestrated locally against
Athena (not Glue jobs — these are lightweight aggregate queries; spinning up
Spark 488 times over would be slow and needlessly expensive).

## Methodology — read this before trusting the numbers

The dataset arrived as one 100M-row historical backfill, not incrementally.
To get a genuine 60-run sample without needing 60 calendar days, this
**simulates** 60+ daily pipeline runs by executing the real checks against
61 real day-slices of the already-generated data, spread across the full
2-year range (see `pick_run_dates` in `run_simulation.py`).

This is compressed cadence, not fabricated results: every number is a real
query result against real (if historical) data. The one place this needed
help — the referential-integrity, duplicate, and consistency checks have
nothing to catch against the real pipeline, because Week 2 validated it's
genuinely clean (0 orphans, 0 post-merge duplicates) — is handled by
building small, clearly-labeled fault-injection scratch tables
(`inject_faults.py`) for 3 of the 61 simulated days, per the project's own
definition of done ("an injected bad batch is caught and alarms fire").
Every result row in `results/dq_run_log.csv` has an `injected` column so
organic and injected incidents are never conflated.

## Running it

```bash
cd quality
python run_simulation.py --runs 60 --bucket <your-bucket-name>
```

Takes a few minutes (488 Athena queries + 3 CTAS scratch tables). Pushes a
CloudWatch metric per check (namespace `RetailLakehouse/DataQuality`,
no per-date dimension — see the comment in `push_metric` for why that
matters for alarms) and writes the full log to `results/dq_run_log.csv`.

To re-push metrics from an existing log without re-running the queries:
`python push_metrics_from_log.py`.

## The two honest findings this run surfaced

- **Null-rate completeness** and **freshness SLA** both consistently miss
  their spec-defined thresholds (0.1% null tolerance, 99% within-5-min SLA)
  — real rates are ~0.33-0.35% and ~98.0-98.1% respectively. This is the
  generator's own designed messiness (≈1% dirty records, 2% late arrivals)
  showing up honestly rather than being tuned away. See `RESULTS.md`.
- **Schema drift** correctly caught the one real transition in the data
  (`promotion_code` going from always-null to populated on 2025-01-05) —
  and this is *not* the physical column-addition scenario originally
  assumed when the generator was built. See the correction in
  `docs/RUNBOOK.md` for what actually happens and why the check was
  rewritten around population rate instead of column presence.

## What's not built

- Real daily cadence (`period=86400` in the CloudWatch alarms) — set to
  300s instead so the alarm mechanism could actually be verified within
  this session; a live pipeline would use the daily period.
- The price-distribution KS test never found a shift because the generator
  samples `unit_price` from a static base price throughout the whole
  dataset (not tied to the SCD2 price history) — genuinely nothing to
  detect here, not a check that silently doesn't work. Documented rather
  than papered over with an injected fault, since inventing statistical
  drift felt like a step too far from "real numbers, real corruption where
  needed."
