.PHONY: teardown

teardown:
	aws redshift-serverless delete-workgroup --workgroup-name perf-wg || true
	aws redshift-serverless delete-namespace --namespace-name perf-ns || true
	aws kinesis delete-stream --stream-name order-events || true
	aws emr-serverless list-applications --query 'applications[].id' \
	  --output text | xargs -n1 -I{} aws emr-serverless stop-application --application-id {} || true
	@echo "Teardown complete. Verify in Cost Explorer tomorrow."
