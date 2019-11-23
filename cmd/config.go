package main

import (
	"fmt"

	"github.com/dimitarvdimitrov/sporkfs/spork"
)

func newSporkConfig(dir string) spork.Config {
	return spork.Config{
		InventoryLocation: fmt.Sprintf("%s/%s", dir, "inventory"),
		DataLocation:      fmt.Sprintf("%s/%s", dir, "data"),
	}
}
