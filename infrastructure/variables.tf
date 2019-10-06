terraform {
  backend "s3" {
    bucket = "spork-tfstate"
    key = "terraform.tfstate"
    region = "eu-central-1"
  }
}

variable "test_nodes" {
  type = number
  default = 3
}
