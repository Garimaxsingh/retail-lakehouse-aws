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
Kinesis record *is* one fact row, no transformation needed.

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

## The `dim_product` MERGE: two statements, not one

Iceberg can't both close out an existing current version AND insert the new
version in a single `MERGE INTO` against the same matched natural key —
whichever branch executes first leaves the other in an inconsistent state
against a table that's being matched on `is_current = true`. The pattern
(see `merge_price_changes_incremental()` in `build_dimensions_and_fact.py`):

```sql
-- Step 1: close out rows whose price actually changed
MERGE INTO dim_product t USING incoming_changes s
  ON t.parent_asin = s.parent_asin AND t.is_current = true
  WHEN MATCHED AND t.price <> s.new_price THEN
    UPDATE SET t.valid_to = s.change_ts, t.is_current = false;

-- Step 2: insert the new current version
MERGE INTO dim_product t USING incoming_changes s
  ON t.parent_asin = s.parent_asin AND t.valid_from = s.change_ts
  WHEN NOT MATCHED THEN
    INSERT (...) VALUES (...);
```

**Important distinction actually implemented in this project**: the script
above is for *incremental* runs (a real daily batch that only sees "what
changed since yesterday"). The **initial load** — reconstructing two years
of pre-existing history from the generator's `product_price_changes` feed
in one shot — is a different problem and uses a bulk window-function
reconstruction instead (`build_dim_product()`): every price-change row
becomes a version boundary via `LEAD()` over `change_ts` per
`parent_asin`, with the version before the first recorded change getting
`old_price` as its value. Both are real, both are in the repo; conflating
"replay two years of history at once" with "apply today's new rows" would
have been the wrong abstraction.

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

`quantity`, `gross_amount`, `discount_amount`, and `net_amount` are
additive — they can be summed across any slice. `unit_price` cannot (the
average of two unit prices is not a meaningful "unit price"), but it's kept
in the fact table anyway for line-item-level audit ("what did this specific
order actually pay per unit") without a join back to `dim_product`, which
would give the *current* price, not what was actually paid.

## What's deliberately not built (Week 2 scope)

- `dim_seller` attribute-change history (see above)
- The incremental MERGE path is written but not exercised — every run so
  far has been a full historical bulk load, since the generator produced
  the full two-year dataset in one shot rather than arriving incrementally
- No holiday calendar in `dim_date.is_holiday` (always `false`) — no data
  source for this was wired in; flagged rather than hardcoded fake values
