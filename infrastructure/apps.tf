provider "kubernetes" {
  host                   = digitalocean_kubernetes_cluster.spork-cluster.kube_config.0.host

  client_certificate     = base64decode(digitalocean_kubernetes_cluster.spork-cluster.kube_config.0.client_certificate)
  client_key             = base64decode(digitalocean_kubernetes_cluster.spork-cluster.kube_config.0.client_key)
  cluster_ca_certificate = base64decode(digitalocean_kubernetes_cluster.spork-cluster.kube_config.0.cluster_ca_certificate)
  version = "~> 1.9"
  load_config_file = false
}

resource "kubernetes_deployment" "example" {
  metadata {
    name = "terraform-example"
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "squid"
      }
    }

    template {
      metadata {
        labels = {
          app = "squid"
          version = var.squid_version
        }
      }

      spec {
        container {
          image   = "busybox"
          name    = "squid"
          command = ["sleep", "1000000"]
        }
      }
    }
  }
}