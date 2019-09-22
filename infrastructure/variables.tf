terraform {
  backend "s3" {
    bucket = "spork-tfstate"
    key = "terraform.tfstate"
    region = "eu-central-1"
  }
}

variable "ceph_nodes" {
  type = number
  default = 3
}

variable "k8s_nodes" {
  type = number
  default = 2
}

variable "squid_version" {
  type = string
}
