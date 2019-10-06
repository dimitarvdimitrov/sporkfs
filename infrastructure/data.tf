data "digitalocean_volume" "test-volumes" {
  count = var.test_nodes
  name = "ceph-vol-${count.index+1}"
}

data "digitalocean_ssh_key" "linux-laptop" {
  name = "Laptop Linux"
}

data "digitalocean_droplet_snapshot" "test-node-image" {
  name = "test-node"
}
