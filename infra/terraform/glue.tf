# One Glue database per layer, each rooted at its own S3 prefix. Iceberg tables
# are created by the Spark job through the Glue catalog
# (catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog), not by Terraform:
# table schemas evolve at run time and do not belong in infrastructure state.
resource "aws_glue_catalog_database" "layer" {
  for_each = toset(local.layers)

  name         = "${replace(var.name_prefix, "-", "_")}_${each.key}"
  description  = "lakehouse-cdc-streaming ${each.key} layer (Iceberg)"
  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/${each.key}/"
  tags         = local.tags
}
