# AWS Glue ETL Jobs (Spark) — the batch compute layer, chosen over EMR
# Serverless after EMR hit a SubscriptionRequiredException on this
# brand-new account (see docs/COST.md). The project's own tech-stack
# decision table already named Glue Jobs as an equally valid choice
# ("Glue Jobs also fine — pick one and justify it"), so this isn't a
# scope compromise, just the other pre-approved option.

data "aws_iam_policy_document" "glue_job_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${var.project_name}-glue-job"
  assume_role_policy = data.aws_iam_policy_document.glue_job_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_job_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job_s3_access" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_job_s3" {
  name   = "${var.project_name}-glue-job-s3"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job_s3_access.json
}

# Kinesis Data Streams hit the same SubscriptionRequiredException as EMR
# Serverless on this account (see docs/COST.md) — the streaming source is
# an S3 "landing" prefix instead (Spark Structured Streaming's file source),
# so no Kinesis IAM permissions are needed. S3 access is already covered by
# glue_job_s3 above.

resource "aws_s3_object" "scripts" {
  for_each = fileset("${path.module}/../jobs/batch", "*.py")

  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/batch/${each.value}"
  source = "${path.module}/../jobs/batch/${each.value}"
  etag   = filemd5("${path.module}/../jobs/batch/${each.value}")
}

resource "aws_glue_job" "raw_to_staging" {
  name              = "${var.project_name}-raw-to-staging"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 90

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.scripts["raw_to_staging.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket}/staging --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--bucket"                           = aws_s3_bucket.lakehouse.bucket
    "--staging_database"                 = aws_glue_catalog_database.staging.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

resource "aws_glue_job" "staging_to_curated" {
  name              = "${var.project_name}-staging-to-curated"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 90

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.scripts["build_dimensions_and_fact.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--bucket"                           = aws_s3_bucket.lakehouse.bucket
    "--staging_database"                 = aws_glue_catalog_database.staging.name
    "--curated_database"                 = aws_glue_catalog_database.curated.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

resource "aws_glue_job" "reconcile" {
  name              = "${var.project_name}-reconcile"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.scripts["reconcile.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--staging_database"                 = aws_glue_catalog_database.staging.name
    "--curated_database"                 = aws_glue_catalog_database.curated.name
    "--reconcile_days"                   = "2"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

resource "aws_glue_job" "backfill_range" {
  name              = "${var.project_name}-backfill-range"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 90

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.scripts["backfill_range.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats" = "iceberg"
    "--conf"             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket}/staging --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--bucket"           = aws_s3_bucket.lakehouse.bucket
    "--staging_database" = aws_glue_catalog_database.staging.name
    # start_date/end_date are placeholders — every real invocation overrides
    # them (see dags/backfill_dag.py), Terraform just needs a default so the
    # job definition is valid.
    "--start_date"                       = "2024-01-01"
    "--end_date"                         = "2024-01-01"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

resource "aws_s3_object" "streaming_scripts" {
  for_each = fileset("${path.module}/../jobs/streaming", "*.py")

  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/streaming/${each.value}"
  source = "${path.module}/../jobs/streaming/${each.value}"
  etag   = filemd5("${path.module}/../jobs/streaming/${each.value}")
}

resource "aws_glue_job" "stream_order_events" {
  name              = "${var.project_name}-stream-order-events"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 2880 # Glue's max — this job is stopped manually, not by timeout

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.streaming_scripts["stream_order_events.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--bucket"                           = aws_s3_bucket.lakehouse.bucket
    "--curated_database"                 = aws_glue_catalog_database.curated.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

output "glue_job_names" {
  value = {
    raw_to_staging      = aws_glue_job.raw_to_staging.name
    staging_to_curated  = aws_glue_job.staging_to_curated.name
    reconcile           = aws_glue_job.reconcile.name
    stream_order_events = aws_glue_job.stream_order_events.name
    backfill_range      = aws_glue_job.backfill_range.name
  }
}
