provider "digitalocean" {
  version = "~> 1.7"
}

resource "digitalocean_droplet" "ceph-nodes" {
  count = var.test_nodes
  image = data.digitalocean_droplet_snapshot.test-node-image.id
  name = "ceph-${count.index+1}"
  region = "lon1"
  size = "s-1vcpu-1gb"
  volume_ids = [data.digitalocean_volume.test-volumes[count.index].id]
  ssh_keys = [data.digitalocean_ssh_key.linux-laptop.id]
  private_networking = true
  monitoring = true
}
