# SeaweedFS all-in-one on a single AWS instance.
#
# Demonstrates the layered design directly: call the cloud-agnostic core to
# render cloud-init for a `weed server` node, then attach it to one instance
# with a protected data disk. No wrapper module needed for the simple case.
#
#   tofu init && tofu validate
#   tofu apply   # requires AWS credentials + a weed AMI

terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.40"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}
variable "vpc_id" {
  type = string
}
variable "subnet_id" {
  type = string
}
variable "availability_zone" {
  type    = string
  default = "us-east-1a"
}
variable "ami_id" {
  type = string
}
variable "private_ip" {
  type    = string
  default = "10.0.1.50"
}

module "core" {
  source      = "../../modules/core"
  weed_binary = "/usr/bin/weed"

  # disable the distributed tiers; run a single all-in-one process
  master = { enabled = false }
  volume = { enabled = false }
  filer  = { enabled = false }

  all_in_one = {
    enabled = true
    nodes   = { a0 = { address = var.private_ip, data_dir = "/data" } }
    s3      = { enabled = true }
  }

  s3_identities = [{
    name       = "anonymous"
    access_key = ""
    secret_key = ""
    actions    = ["Read", "List"]
  }]
}

resource "aws_network_interface" "aio" {
  subnet_id       = var.subnet_id
  private_ips     = [var.private_ip]
  security_groups = [aws_security_group.aio.id]
  tags            = { Name = "seaweedfs-all-in-one" }
}

resource "aws_security_group" "aio" {
  name_prefix = "seaweedfs-aio-"
  vpc_id      = var.vpc_id
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "s3" {
  security_group_id = aws_security_group.aio.id
  cidr_ipv4         = "10.0.0.0/8"
  ip_protocol       = "tcp"
  from_port         = 8333
  to_port           = 8333
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.aio.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_instance" "aio" {
  ami           = var.ami_id
  instance_type = "m5.large"
  user_data     = module.core.cloud_init_by_node["all-in-one-a0"]

  network_interface {
    network_interface_id = aws_network_interface.aio.id
    device_index         = 0
  }
  metadata_options {
    http_tokens = "required"
  }
  tags = { Name = "seaweedfs-all-in-one", Role = "all-in-one" }
}

resource "aws_ebs_volume" "data" {
  availability_zone = var.availability_zone
  size              = 200
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "seaweedfs-all-in-one-data" }
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_volume_attachment" "data" {
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.aio.id
}

output "instance_id" {
  value = aws_instance.aio.id
}
