# Reference mapping of the local stack onto AWS. Not applied anywhere; see README.md.
#
#   MinIO bucket           -> S3 bucket (this file)
#   Iceberg REST catalog   -> Glue Data Catalog databases (glue.tf)
#   Redpanda               -> MSK Serverless with IAM auth (msk.tf)
#   Spark / Connect creds  -> IAM roles scoped to the above (iam.tf)

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
  partition  = data.aws_partition.current.partition

  layers = ["bronze", "silver", "gold", "ops"]

  tags = merge(var.tags, {
    Project   = "lakehouse-cdc-streaming"
    ManagedBy = "terraform"
  })
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.name_prefix}-lakehouse-${local.account_id}"
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    bucket_key_enabled = true

    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

# Versioning is the safety net under Iceberg maintenance: expire_snapshots and
# remove_orphan_files delete objects, and a mistake there is recoverable only
# if the old versions still exist.
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Data lifecycle belongs to Iceberg (snapshot expiry), not to S3 rules that
  # would delete files a snapshot still references. S3 only cleans up after it.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }

  depends_on = [aws_s3_bucket_versioning.lakehouse]
}

resource "aws_s3_bucket_policy" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = data.aws_iam_policy_document.lakehouse_bucket.json
}

data "aws_iam_policy_document" "lakehouse_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}
