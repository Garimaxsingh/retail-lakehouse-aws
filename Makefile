.PHONY: teardown sync-raw crawl-raw

BUCKET := $(shell cd infra && terraform output -raw bucket_name)

sync-raw:
	aws s3 sync generator/data/raw/order_events s3://$(BUCKET)/raw/order_events --only-show-errors
	aws s3 sync generator/data/raw/product_price_changes s3://$(BUCKET)/raw/product_price_changes --only-show-errors
	@echo "Synced generator output to s3://$(BUCKET)/raw/"

crawl-raw:
	aws glue start-crawler --name retail-lakehouse-raw-crawler
	@echo "Crawler started — check status with: aws glue get-crawler --name retail-lakehouse-raw-crawler"

# Rewritten after the EMR Serverless / Kinesis pivot (see docs/RUNBOOK.md) —
# this project's only continuously-billing resource is the streaming Glue
# job; everything else (S3, Glue Catalog, Athena workgroup, batch/reconcile
# Glue job *definitions*) only costs money while actually running a query
# or job, not at rest. Redshift/Kinesis/EMR were never created on this
# account (all three hit SubscriptionRequiredException or were substituted),
# so there is deliberately nothing to tear down for them.
teardown:
	@echo "Stopping any running streaming job (the only continuously-billing resource)..."
	aws glue get-job-runs --job-name retail-lakehouse-stream-order-events --max-results 5 \
	  --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text | \
	  xargs -n1 -I{} aws glue batch-stop-job-run --job-name retail-lakehouse-stream-order-events --job-run-ids {} || true
	@echo "Teardown complete. Verify in Cost Explorer tomorrow. S3/Glue Catalog/Athena workgroup are left in place (free at rest)."
