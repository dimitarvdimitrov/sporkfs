<!-- TODO this -->
These are materials I used to setup my demo with 6 spork nodes in Digital Ocean.

You can use it as well.

1) Apply the terraform.
2) Run `make-inventory.sh`
3) `ansible-playbook -i inventory setup-spork.yaml`

This will get you a cluster with 6 nodes with redundancy factor of 3.
