terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "5.100.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "random_pet" "suffix" {
  length    = 2
  separator = "-"
}
