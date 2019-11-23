package main

import (
	"flag"
	"fmt"

	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func main() {
	defer log.Sync()

	flag.Parse()
	mountpoint := flag.Arg(0)
	dataDir := flag.Arg(1)
	log.Infof("trying to mount sporkfs at %s...", mountpoint)

	c, err := fuse.Mount(mountpoint,
		fuse.FSName("sporkfs"),
		fuse.VolumeName("sporkfs"),
	)

	if err != nil {
		log.Fatal("couldn't start up: ", err)
	}
	defer c.Close()

	log.Infof("starting sporkfs...")
	vfs := sfuse.Fs{
		S: spork.New(newSporkConfig(dataDir)),
	}
	defer vfs.Destroy()

	err = fs.Serve(c, vfs)
	if err != nil {
		log.Fatal("serve: ", err)
	}

	log.Info("stopping...")
}

func newSporkConfig(dir string) spork.Config {
	return spork.Config{
		InventoryLocation: fmt.Sprintf("%s/%s", dir, "inventory"),
		DataLocation:      fmt.Sprintf("%s/%s", dir, "data"),
	}
}
