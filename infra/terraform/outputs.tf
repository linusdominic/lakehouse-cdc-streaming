output "lakehouse_bucket" {
  description = "S3 bucket holding the Iceberg warehouse and the streaming checkpoints."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "warehouse_uri" {
  description = "Value for spark.sql.catalog.<name>.warehouse."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/"
}

output "checkpoint_uri" {
  description = "Structured Streaming checkpoint location."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/checkpoints/cdc/"
}

output "glue_databases" {
  description = "Glue database name per layer."
  value       = { for layer, db in aws_glue_catalog_database.layer : layer => db.name }
}

output "msk_cluster_arn" {
  description = "MSK Serverless cluster ARN. Fetch the IAM bootstrap brokers with `aws kafka get-bootstrap-brokers`."
  value       = aws_msk_serverless_cluster.cdc.arn
}

output "streaming_role_arn" {
  description = "Execution role for the Spark streaming job (EMR Serverless)."
  value       = aws_iam_role.streaming.arn
}

output "connect_role_arn" {
  description = "Task role for the Debezium Kafka Connect workers (ECS)."
  value       = aws_iam_role.connect.arn
}
