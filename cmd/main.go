package main

import (
	"context"
	"flag"
	"os"
	"os/signal"

	"github.com/BurntSushi/toml"
	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func main() {
	defer log.Sync()

	flag.Parse()
	cfgLocation := flag.Arg(0)

	cfg := parseConfig(cfgLocation)

	ctx, cancel := context.WithCancel(context.Background())

	invFiles := make(chan *store.File)
	sporkService, err := spork.New(ctx, cancel, cfg, invFiles)
	if err != nil {
		log.Fatal("inti: ", err)
	}

	vfs := sfuse.NewFS(&sporkService, invFiles)

	startFuseServer(ctx, cancel, cfg.MountPoint, vfs)
	defer vfs.Destroy()
	handleOsSignals(ctx, cancel)
	unmountWhenDone(ctx, cfg.MountPoint)

	<-ctx.Done()
	log.Info("stopping spork...")
}

func handleOsSignals(ctx context.Context, cancel context.CancelFunc) {
	go func() {
		signals := make(chan os.Signal)
		signal.Notify(signals, os.Kill, os.Interrupt)

		select {
		case <-signals:
		case <-ctx.Done():
		}

		cancel()
	}()
}

func unmountWhenDone(ctx context.Context, mountpoint string) {
	go func() {
		<-ctx.Done()
		if err := fuse.Unmount(mountpoint); err != nil {
			log.Errorf("unmount: %s", err)
		}
	}()
}

func startFuseServer(ctx context.Context, cancel context.CancelFunc, mountpoint string, vfs sfuse.Fs) {
	log.Infof("mounting sporkfs at %s...", mountpoint)
	fuseConn, err := fuse.Mount(mountpoint,
		fuse.FSName("sporkfs"),
		fuse.VolumeName("sporkfs"),
	)
	if err != nil {
		log.Fatal("couldn't mount: ", err)
	}
	log.Infof("mount successful")

	fuseServer := fs.New(fuseConn, &fs.Config{
		Debug: func(m interface{}) { log.Debug(m) },
	})

	go func() {
		log.Info("sporkfs started")
		if err := fuseServer.Serve(vfs); err != nil {
			log.Error("serve: ", err)
		}
		cancel()
	}()

	go vfs.WatchInvalidations(ctx, fuseServer)
}

func parseConfig(dir string) (cfg spork.Config) {
	_, err := toml.DecodeFile(dir, &cfg)
	if err != nil {
		log.Fatalf("decoding config: %s", err)
	}
	return
}
