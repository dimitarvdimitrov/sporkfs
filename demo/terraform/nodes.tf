provider "digitalocean" {
  version = "~> 1.7"
}

variable "test_nodes" {
  type = number
  default = 6
}

data "digitalocean_ssh_key" "linux-laptop" {
  name = "Laptop Linux"
}

resource "digitalocean_droplet" "spork-nodes" {
  count = var.test_nodes
  image = "ubuntu-18-04-x64"
  name = "spork-node-${count.index+1}"
  region = "lon1"
  size = "s-1vcpu-1gb"
  ssh_keys = [data.digitalocean_ssh_key.linux-laptop.id]
  private_networking = true
  monitoring = true
}
