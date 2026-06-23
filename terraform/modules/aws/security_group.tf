# Cluster security group + the SeaweedFS port matrix.
# Intra-cluster: all TCP allowed within the SG (master 9333/19333, volume
# 8080/18080, filer 8888/18888, admin 33646, ...). Client-facing: only S3 (8333)
# and filer (8888). Metrics ports (9327) are never opened to clients.

resource "aws_security_group" "cluster" {
  name_prefix = "${var.name}-cluster-"
  description = "SeaweedFS cluster"
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name}-cluster" })

  lifecycle {
    create_before_destroy = true
  }
}

# All intra-cluster TCP via SG self-reference.
resource "aws_vpc_security_group_ingress_rule" "intra_cluster" {
  security_group_id            = aws_security_group.cluster.id
  referenced_security_group_id = aws_security_group.cluster.id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
  description                  = "intra-cluster (http + gRPC for all roles)"
}

resource "aws_vpc_security_group_ingress_rule" "s3" {
  for_each          = toset(var.client_ingress_cidrs)
  security_group_id = aws_security_group.cluster.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 8333
  to_port           = 8333
  description       = "S3 gateway"
}

resource "aws_vpc_security_group_ingress_rule" "filer" {
  for_each          = toset(var.client_ingress_cidrs)
  security_group_id = aws_security_group.cluster.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 8888
  to_port           = 8888
  description       = "filer HTTP"
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each          = toset(var.ssh_ingress_cidrs)
  security_group_id = aws_security_group.cluster.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  description       = "SSH"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.cluster.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "all egress"
}
