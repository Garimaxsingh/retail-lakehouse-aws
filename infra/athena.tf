# Athena workgroup for ad-hoc validation queries + (later) the perf-teardown
# project's E1/E2/E3/E5/E6 experiments. Per-query scan limit is a hard
# guardrail against a careless SELECT * — see docs/AWS_SETUP.md §5.

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results-${random_id.bucket_suffix.hex}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {
      prefix = ""
    }
    expiration {
      days = 14
    }
  }
}

resource "aws_athena_workgroup" "main" {
  name = "${var.project_name}-wg"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Hard per-query cap: 10 GB scanned. At $5/TB this bounds a single
    # careless query to ~$0.05 — cheap to hit, expensive to hit by accident
    # a hundred times over a session.
    bytes_scanned_cutoff_per_query = 10 * 1024 * 1024 * 1024

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }
  }
}

output "athena_workgroup" {
  value = aws_athena_workgroup.main.name
}
