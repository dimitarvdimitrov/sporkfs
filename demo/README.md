# Demo

These are materials I used to setup my demo with 6 spork nodes in Digital Ocean.
This will get you a cluster with 6 nodes with redundancy factor of 3.

The demo also installs `tree` and copies some scripts I used (like watching a file or directory and getting the IP of
the current leader), you can delete those tasks from the playbook.

## Set up
1) Apply the terraform
2) `cd ansible`
3) `./make-inventory.sh`
    > Note: This will create an inventory with all of your droplets in DO
4) `ansible-playbook -i inventory setup-spork.yaml`
