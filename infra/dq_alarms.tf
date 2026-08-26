# Data-quality alerting: CloudWatch alarms on the metrics quality/*.py
# publishes, fanning out to a single SNS topic with an email subscription.
# ~$0.10/alarm/month — trivial at this scale (8 alarms).

resource "aws_sns_topic" "dq_alerts" {
  name = "${var.project_name}-dq-alerts"
}

resource "aws_sns_topic_subscription" "dq_alerts_email" {
  topic_arn = aws_sns_topic.dq_alerts.arn
  protocol  = "email"
  endpoint  = "garimasingh19092003@gmail.com"
}

locals {
  # Per the project spec's DQ table: some checks block the pipeline, some
  # just alarm. All get a CloudWatch alarm either way — "block" happens in
  # the job itself (see raw_to_staging.py's quarantine routing), the alarm
  # here is the notification layer for both classes.
  dq_check_metrics = [
    "volume_vs_7d_mean",
    "null_rate_product_id",
    "referential_integrity_fact_to_dim_product",
    "net_amount_consistency",
    "duplicate_event_id_post_merge",
    "freshness_sla_5min",
    "schema_drift_vs_previous",
    "price_distribution_shift_ks",
  ]
}

resource "aws_cloudwatch_metric_alarm" "dq_check_failed" {
  for_each = toset(local.dq_check_metrics)

  alarm_name  = "${var.project_name}-dq-${each.value}"
  namespace   = "RetailLakehouse/DataQuality"
  metric_name = each.value
  statistic   = "Minimum"
  period      = 300 # 5 min — short enough to actually verify the alarm fires
  # against today's compressed 60-run simulation, which lands all its
  # datapoints within minutes rather than spread over real days. A real
  # daily-cadence pipeline would want period=86400 instead.
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_description   = "Data quality check '${each.value}' failed at least once in the evaluation window (metric value 0 = fail, 1 = pass)."
  alarm_actions       = [aws_sns_topic.dq_alerts.arn]
  ok_actions          = [aws_sns_topic.dq_alerts.arn]
}

output "dq_alerts_topic_arn" {
  value = aws_sns_topic.dq_alerts.arn
}
