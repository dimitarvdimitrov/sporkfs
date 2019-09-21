provider "digitalocean" {}

data "digitalocean_volume" "ceph-volumes" {
  count = var.ceph-nodes
  name = "ceph-vol-${count.index+1}"
}

data "digitalocean_volume" "db-volume" {
  name = "sporkdb-vol"
}

data "digitalocean_ssh_key" "linux-laptop" {
  name = "Laptop Linux"
}

data "digitalocean_droplet_snapshot" "sporkdb-image" {
  name = "postgresql"
}

resource "digitalocean_droplet" "db-node" {
  name = "sporkdb"
  image = data.digitalocean_droplet_snapshot.sporkdb-image.id
  region = "lon1"
  size = "s-1vcpu-1gb"
  volume_ids = [data.digitalocean_volume.db-volume.id]
  ssh_keys = [data.digitalocean_ssh_key.linux-laptop.id]
  private_networking = true
  monitoring = true
}

resource "digitalocean_droplet" "ceph-nodes" {
  count = var.ceph-nodes
  image = "ubuntu-18-04-x64"
  name = "ceph-${count.index+1}"
  region = "lon1"
  size = "s-1vcpu-1gb"
  volume_ids = [data.digitalocean_volume.ceph-volumes[count.index].id]
  ssh_keys = [data.digitalocean_ssh_key.linux-laptop.id]
  private_networking = true
  monitoring = true
}

resource "digitalocean_kubernetes_cluster" "spork-cluster" {
  name = "spork-cluster"
  version = "1.15.3-do.2"
  region = "lon1"
  node_pool {
    name = "k8s-worker"
    node_count = var.k8s-nodes
    size = "s-1vcpu-2gb"
  }
}
