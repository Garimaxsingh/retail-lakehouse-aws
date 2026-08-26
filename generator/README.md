# Order-Event Generator

Seeded, reproducible, runs entirely on your laptop — no AWS calls. Produces
`fact_order_item`-shaped events (grain: one row per order line item per
status transition) plus a `product_price_changes` feed.

## One-time setup

```bash
cd retail-lakehouse-aws
python -m venv .venv && .venv/Scripts/activate   # or source .venv/bin/activate on macOS/Linux
pip install -r requirements.txt

# Product catalog (public, no auth needed)
python generator/data_sources/fetch_amazon_metadata.py Electronics Home_and_Kitchen Clothing_Shoes_and_Jewelry

# Sellers/customers (needs a Kaggle account token — see the script's docstring)
python generator/data_sources/fetch_olist.py
```

If you don't have Olist yet, pass `--dev-fixture` below to smoke-test against
a tiny synthetic seller/customer set instead — clearly not real data, just
enough to prove the pipeline works before Kaggle is set up.

## Run

```bash
cd generator
python order_events.py --seed 42 --target-line-items 25000000
```

`--target-line-items` is order *line items*, not output rows — each expands
into 2-4 status-transition events (avg ~3.65x), so 25M line items produces
roughly the 100M fact rows the project targets. Output lands partitioned by
`ingest_date` under `generator/data/raw/order_events/`.

Key flags: `--seed` (reproducibility — never regenerate with a different
seed once downstream numbers depend on this run), `--chunk-size` (rows
processed/written per batch, tune for your machine's RAM), `--start-date`/
`--end-date` (24-month window), `--schema-evolution-date` (when
`promotion_code` starts appearing), `--max-products` (cap catalog size for a
faster dev run).

## Realism properties (see project README for the why)

| Property | How |
|---|---|
| Zipfian product skew | `popularity.py` solves for the Zipf exponent that puts exactly 80% of mass on the top 5% of the catalog, rather than hand-tuning |
| Seasonal/hour curve | Q4 months weighted 1.5x; hour-of-day sine wave peaking ~14:00 |
| Late arrivals (2%) | `ingest_ts` delayed 20-90 min past `event_ts` — beyond the streaming path's 15-min watermark |
| Duplicates (0.5%) | Same `event_id` re-emitted with a later `ingest_ts` |
| Dirty records (~1%) | Null `product_id`, negative `quantity`, or `seller_id = "seller_UNKNOWN"` |
| Price drift | Separate `product_price_changes` feed, ~3% of catalog/month |
| Schema evolution | `promotion_code` exists in every row but is only ever populated from `--schema-evolution-date` onward (0% -> >0% non-null, not a physical column-presence difference — see the correction in `docs/RUNBOOK.md`) |

## Once generated

```bash
cd ..   # back to retail-lakehouse-aws/
make sync-raw    # aws s3 sync to the Terraform-created bucket
make crawl-raw   # populate the Glue Catalog raw database
```
