#!/bin/bash

doctl compute droplet list --no-header --format=PublicIPv4 | sort > inventory
