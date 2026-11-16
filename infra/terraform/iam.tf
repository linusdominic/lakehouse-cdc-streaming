locals {
  msk_cluster_name = aws_msk_serverless_cluster.cdc.cluster_name
  msk_cluster_uuid = element(split("/", aws_msk_serverless_cluster.cdc.arn), 2)
  msk_arn_prefix   = "arn:${local.partition}:kafka:${local.region}:${local.account_id}"

  msk_cdc_topic_arn     = "${local.msk_arn_prefix}:topic/${local.msk_cluster_name}/${local.msk_cluster_uuid}/cdc.*"
  msk_connect_topic_arn = "${local.msk_arn_prefix}:topic/${local.msk_cluster_name}/${local.msk_cluster_uuid}/_connect*"
  msk_any_group_arn     = "${local.msk_arn_prefix}:group/${local.msk_cluster_name}/${local.msk_cluster_uuid}/*"

  glue_database_arns = [for db in aws_glue_catalog_database.layer : db.arn]
  glue_table_arns = [
    for db in aws_glue_catalog_database.layer :
    "arn:${local.partition}:glue:${local.region}:${local.account_id}:table/${db.name}/*"
  ]
}

# ---------------------------------------------------------------------------
# Streaming job (Spark on EMR Serverless): reads the CDC topics, writes Iceberg.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "streaming_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "streaming" {
  name               = "${var.name_prefix}-cdc-streaming"
  assume_role_policy = data.aws_iam_policy_document.streaming_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "streaming" {
  statement {
    sid       = "ListLakehouseBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  # Data, Iceberg metadata and the Structured Streaming checkpoint.
  statement {
    sid     = "ReadWriteWarehouseAndCheckpoints"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/warehouse/*",
      "${aws_s3_bucket.lakehouse.arn}/checkpoints/*",
    ]
  }

  statement {
    sid = "GlueCatalogForIceberg"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = concat(
      ["arn:${local.partition}:glue:${local.region}:${local.account_id}:catalog"],
      local.glue_database_arns,
      local.glue_table_arns,
    )
  }

  statement {
    sid       = "KafkaConnectToCluster"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.cdc.arn]
  }

  statement {
    sid       = "KafkaReadCdcTopics"
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
    resources = [local.msk_cdc_topic_arn]
  }
}

resource "aws_iam_role_policy" "streaming" {
  name   = "lakehouse-access"
  role   = aws_iam_role.streaming.id
  policy = data.aws_iam_policy_document.streaming.json
}

# ---------------------------------------------------------------------------
# Debezium (Kafka Connect on ECS/Fargate): writes the CDC topics. No S3, no Glue.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "connect_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "connect" {
  name               = "${var.name_prefix}-cdc-connect"
  assume_role_policy = data.aws_iam_policy_document.connect_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "connect" {
  statement {
    sid       = "KafkaConnectToCluster"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.cdc.arn]
  }

  statement {
    sid = "KafkaWriteCdcAndConnectTopics"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData",
    ]
    resources = [local.msk_cdc_topic_arn, local.msk_connect_topic_arn]
  }

  # Connect workers coordinate through a consumer group.
  statement {
    sid       = "KafkaConnectWorkerGroup"
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = [local.msk_any_group_arn]
  }
}

resource "aws_iam_role_policy" "connect" {
  name   = "kafka-access"
  role   = aws_iam_role.connect.id
  policy = data.aws_iam_policy_document.connect.json
}
