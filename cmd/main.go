package main

import (
	"flag"

	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func main() {
	defer log.Sync()

	flag.Parse()
	mountpoint := flag.Arg(0)
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

	err = fs.Serve(c, sfuse.Fs{})
	if err != nil {
		log.Fatal("couldn't listen start fuse server")
	}

	log.Info("stopping")
}
