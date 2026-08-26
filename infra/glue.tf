# Three Glue databases matching the zone contract. Raw is crawler-populated
# for exploration; staging/curated tables are Iceberg tables registered by
# the Spark jobs themselves (Glue Catalog as the Iceberg catalog backend).

resource "aws_glue_catalog_database" "raw" {
  name = "${replace(var.project_name, "-", "_")}_raw"
}

resource "aws_glue_catalog_database" "staging" {
  name = "${replace(var.project_name, "-", "_")}_staging"
}

resource "aws_glue_catalog_database" "curated" {
  name = "${replace(var.project_name, "-", "_")}_curated"
}

# --- Crawler role: crawls the raw zone only, to catalog incoming JSONL/CSV
# for initial exploration before the batch job types and dedupes it.

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project_name}-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_crawler_s3_access" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/raw/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }
}

resource "aws_iam_role_policy" "glue_crawler_s3" {
  name   = "${var.project_name}-glue-crawler-s3"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler_s3_access.json
}

resource "aws_glue_crawler" "raw" {
  name          = "${var.project_name}-raw-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.raw.name

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/raw/"
  }

  # Run manually via `aws glue start-crawler` rather than on a schedule —
  # a scheduled crawler is a recurring cost with no benefit at this data volume.
}

output "glue_databases" {
  value = {
    raw     = aws_glue_catalog_database.raw.name
    staging = aws_glue_catalog_database.staging.name
    curated = aws_glue_catalog_database.curated.name
  }
}
