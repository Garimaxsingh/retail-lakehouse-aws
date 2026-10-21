# Data Model

## Grain

**`fact_order_item`: one row per order line item per status transition.**

A line item that goes `created -> approved -> shipped -> delivered` produces
four fact rows, not one. This was decided upfront (see the project README)
because the alternative grain — one row per line item, with a mutable
"current status" column — can't answer "how long did items typically sit in
`approved` before shipping" without a separate events table anyway. Making
the transitions the fact rows means that question is just a `GROUP BY
order_status` away, and it makes the streaming path's job trivial: each
incoming event *is* one fact row, no transformation needed.

**The cost of this grain — and the second fact table that fixes it.** The
money columns (`quantity`, `gross_amount`, `discount_amount`, `net_amount`)
are the line item's values, repeated on every transition row. So they are
**not additive across status**: `SUM(net_amount)` over `fact_order_item`
counts each sale once per transition, about 3.8 times on average. An earlier
version of this document wrongly called them "additive across any slice".
Two correct ways to get revenue:

1. **`fact_order_line`** (built by `jobs/batch/build_fact_order_line.py`): an
   *accumulating snapshot* fact — one row per line item, one timestamp column
   per milestone (`created_ts`, `approved_ts`, `shipped_ts`, `delivered_ts`,
   `canceled_ts`), `current_status`, `hours_to_deliver`, and the money columns
   stored exactly once. Use this for revenue and units.
2. On `fact_order_item`, filter to a single status — `order_status =
   'created'` for bookings, `'delivered'` for realised revenue.

`fact_order_item` remains the right table for process questions (time in
each status, cancellation stage). See RESULTS.md for the measured
overcount ratio.

## Why Iceberg

Same reasoning as most lakehouse projects: ACID `MERGE` (needed for SCD2 and
for idempotent event replay), snapshot/time-travel (useful for "what did
this report look like before the backfill"), and schema evolution without a
full rewrite. The concrete reason it matters *here*: `dim_product` genuinely
needs point-in-time-correct joins (see below), and building that by hand on
plain Parquet would mean maintaining valid_from/valid_to bookkeeping AND a
manual compaction story. Iceberg's snapshot isolation means the batch job
can `createOrReplace()` a table mid-run without a reader ever seeing a
half-written table.

## SCD strategy per dimension

| Dimension | Strategy | Tracked (Type 2) | Overwritten (Type 1) |
|---|---|---|---|
| `dim_product` | **Type 2** | `price`, `price_band`, `category_l1` | `title`, `average_rating` |
| `dim_seller` | Type 2 *(shape only — see note)* | `seller_state`, `seller_tier` (planned) | — |
| `dim_customer` | **Type 1** | — | everything |

**`dim_product` is Type 2** because revenue restatement requires point-in-time
price — if a product was $80 in March and $95 in April, March's revenue
report must join to $80, not today's $95. `title` and `average_rating` are
overwritten because nobody analyzes historical titles or wants a fact row's
revenue attribution to change because a product's rating drifted.

**`dim_customer` is Type 1** deliberately: customer geography changes in
this dataset are corrections (a typo'd zip code, a re-fanned synthetic ID),
not history worth preserving. There's no business question here that needs
"what was this customer's city as of their March order."

**`dim_seller` is SCD2-*shaped* but currently single-version per seller** —
worth being honest about. The synthetic order-event generator doesn't
produce a seller-attribute-change feed analogous to `product_price_changes`
(no seller ever changes state/tier over the generated 2-year window). The
table carries `valid_from`/`valid_to`/`is_current` and is fully MERGE-ready,
but every row today has `valid_from = project epoch` and
`valid_to = 9999-12-31`. This is a documented roadmap gap, not a silent
simplification — see the note in `jobs/batch/build_dimensions_and_fact.py`.

## Incremental `dim_product` updates: one atomic MERGE

Daily runs apply only the new price changes, via `apply_price_changes()` in
`jobs/batch/scd2.py`. It builds a single MERGE source with two kinds of rows:

- **close rows** (`merge_key = parent_asin`): match the current version and
  set `valid_to = first change_ts`, `is_current = false`;
- **insert rows** (`merge_key = NULL`): can never match, so every new version
  is inserted with all attributes (title, categories, price_band, row_hash,
  a new `product_sk`).

```sql
MERGE INTO dim_product t
USING scd2_merge_source s
ON t.parent_asin = s.merge_key AND t.is_current = true
WHEN MATCHED THEN UPDATE SET t.valid_to = s.close_ts, t.is_current = false
WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT (...) VALUES (...)
```

Iceberg commits the whole MERGE as one snapshot, so a reader never sees a
product with zero or two current versions. Before building the source it
drops no-op changes (same price as the previous version), chains several
changes to one product in the same batch in `change_ts` order, ignores
unknown products, and skips changes not newer than the current version — so
applying the same batch twice changes nothing.

**Why it changed.** The first version of this pattern was two separate MERGE
statements (close, then insert). Reviewing it before it ever ran in
production found three defects: it was not atomic (a failure between the
statements left a product with no current version), step 2 inserted a new
version even when the price hadn't changed (two current rows), and the
INSERT left title, categories, price_band and `product_sk` NULL.
`jobs/batch/test_scd2_merge.py` now proves the replacement on a scratch
Iceberg table with the real schema (15 checks, including idempotent
re-application) — see RESULTS.md.

**Initial load is still different**: reconstructing two years of history in
one pass uses a bulk window-function rebuild instead (`build_dim_product()`):
every price-change row becomes a version boundary via `LEAD()` over
`change_ts` per `parent_asin`, and the version before the first change gets
`old_price`.

## Point-in-time correct fact join

`fact_order_item` joins to `dim_product` on:

```sql
e.product_id = p.parent_asin
AND e.event_ts >= p.valid_from
AND e.event_ts <  p.valid_to
```

not on `product_sk` directly and not to `is_current = true`. This is the
entire reason `dim_product` carries `valid_from`/`valid_to` instead of being
overwritten in place — an order placed in March joins to March's price
version, regardless of how many times the product's price has changed
since.

## `unit_price` is kept despite being non-additive

In `fact_order_line`, `quantity`, `gross_amount`, `discount_amount` and
`net_amount` are fully additive. In `fact_order_item` they are additive only
within a single `order_status` (see Grain). `unit_price` is never additive (the
average of two unit prices is not a meaningful "unit price"), but it's kept
in the fact table anyway for line-item-level audit ("what did this specific
order actually pay per unit") without a join back to `dim_product`, which
would give the *current* price, not what was actually paid.

## What's deliberately not built (Week 2 scope)

- `dim_seller` attribute-change history (see above)
- The incremental MERGE is proven on a scratch table, not yet run against
  the real `dim_product` — the generator produced the full two-year dataset
  in one shot, so there is no real "new day of changes" to apply yet
- Order `unit_price` is sampled from the catalog base price, not from the
  `dim_product` version valid at `event_ts`, so the point-in-time join is
  structurally correct but price drift doesn't show up in order prices
- No holiday calendar in `dim_date.is_holiday` (always `false`) — no data
  source for this was wired in; flagged rather than hardcoded fake values
