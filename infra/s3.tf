# Single bucket, three zone prefixes (raw/, staging/, curated/) — matches the
# zone contract in the project README. One bucket keeps IAM and lifecycle
# rules simple; Iceberg tables live under staging/ and curated/ as their own
# table directories.

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-${random_id.bucket_suffix.hex}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Raw zone -> Glacier Instant Retrieval after 30 days (per COST.md guardrails).
# Incomplete multipart uploads deleted after 7 days across the whole bucket —
# these accumulate silently from interrupted `aws s3 sync` runs.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "raw-to-glacier-ir"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 30
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}
