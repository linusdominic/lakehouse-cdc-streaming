# MSK Serverless: no broker sizing, IAM authentication only, pay per use.
# It fits this workload (a handful of CDC topics, modest throughput). The
# limits to know before choosing it over provisioned MSK: IAM auth only, no
# custom broker configuration, and per-cluster caps on partitions and
# throughput.
resource "aws_security_group" "msk" {
  name        = "${var.name_prefix}-msk"
  description = "MSK Serverless brokers for lakehouse-cdc-streaming"
  vpc_id      = var.vpc_id
  tags        = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "msk_iam_from_clients" {
  for_each = toset(var.client_security_group_ids)

  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = each.value
  description                  = "Kafka IAM (SASL) from client security group"
  ip_protocol                  = "tcp"
  from_port                    = 9098
  to_port                      = 9098
}

resource "aws_msk_serverless_cluster" "cdc" {
  cluster_name = "${var.name_prefix}-cdc"
  tags         = local.tags

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}
