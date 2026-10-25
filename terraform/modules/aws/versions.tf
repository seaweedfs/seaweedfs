terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source = "hashicorp/aws"
      # >= 5.40 for the modern aws_vpc_security_group_*_rule resources.
      # Provider >= 6.0 is recommended for the EBS-attachment no-replace behavior;
      # bump the floor here once you standardize on it.
      version = ">= 5.40"
    }
  }
}
