# AWS reference module

This is a reference for how the local stack maps onto AWS. It has not been
applied to any account. It passes `terraform fmt -check` and
`terraform validate` (Terraform 1.9.8, AWS provider 5.100.0, run from the
`hashicorp/terraform:1.9` image); it has not been through `terraform plan`
against real credentials.

| Local | AWS | File |
| --- | --- | --- |
| MinIO bucket `warehouse` | S3 bucket, versioned, SSE-KMS, public access blocked, TLS-only policy | `main.tf` |
| Iceberg REST catalog | Glue Data Catalog, one database per layer | `glue.tf` |
| Redpanda | MSK Serverless, IAM auth | `msk.tf` |
| Spark container | EMR Serverless execution role | `iam.tf` |
| Kafka Connect container | ECS task role | `iam.tf` |

Deliberately left out: the VPC, the EMR Serverless application and job, the
ECS service for Kafka Connect, and the RDS source database. They depend on the
account's network layout and are passed in (`vpc_id`, `private_subnet_ids`,
`client_security_group_ids`) or left to the consuming stack.

What changes in the job configuration on AWS:

```
spark.sql.catalog.lake.type           glue
spark.sql.catalog.lake.warehouse      <warehouse_uri output>
spark.sql.catalog.lake.io-impl        org.apache.iceberg.aws.s3.S3FileIO
CDC_CHECKPOINT_DIR                    <checkpoint_uri output>
kafka.security.protocol               SASL_SSL
kafka.sasl.mechanism                  AWS_MSK_IAM
```

The IAM policies are scoped to this module's bucket prefixes, Glue databases
and `cdc.*` topics. The streaming role can read the CDC topics and write the
lake; the Connect role can write the CDC topics and has no S3 or Glue access.

To check it yourself:

```
docker run --rm -v "$PWD":/tf -w /tf hashicorp/terraform:1.9 fmt -check -recursive
docker run --rm -v "$PWD":/tf -w /tf --entrypoint sh hashicorp/terraform:1.9 \
  -c 'terraform init -backend=false && terraform validate'
```
