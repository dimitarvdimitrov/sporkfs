package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/BurntSushi/toml"
	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"go.uber.org/zap"
)

func main() {
	defer log.Sync()

	flag.Parse()
	cfgLocation := flag.Arg(0)

	cfg := parseConfig(cfgLocation)

	ctx, cancel := context.WithCancel(context.Background())

	invFiles := make(chan *store.File)
	deletedFiles := make(chan *store.File)
	sporkService, err := spork.New(ctx, cancel, cfg, invFiles, deletedFiles)
	if err != nil {
		log.Fatal("init", zap.Error(err))
	}

	vfs := sfuse.NewFS(&sporkService, invFiles, deletedFiles)
	wg := &sync.WaitGroup{}
	startFuseServer(ctx, cancel, cfg.MountPoint, vfs, wg)
	handleOsSignals(ctx, cancel)
	unmountWhenDone(ctx, cfg.MountPoint, wg)

	<-ctx.Done()

	log.Info("shutting down...")
	vfs.Destroy()
	wg.Wait()
	log.Info("bye-bye")
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

func unmountWhenDone(ctx context.Context, mountpoint string, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		<-ctx.Done()
		if err := fuse.Unmount(mountpoint); err != nil {
			log.Error("unmount", zap.Error(err))
		}
		wg.Done()
	}()
}

func startFuseServer(ctx context.Context, cancel context.CancelFunc, mountpoint string, vfs sfuse.Fs, wg *sync.WaitGroup) {
	log.Info(fmt.Sprintf("mounting sporkfs at %s...", mountpoint))
	fuseConn, err := fuse.Mount(mountpoint,
		fuse.FSName("sporkfs"),
		fuse.VolumeName("sporkfs"),
	)
	if err != nil {
		log.Fatal("couldn't mount", zap.Error(err))
	}
	log.Info("mount successful")

	fuseServer := fs.New(fuseConn, &fs.Config{
		Debug: func(m interface{}) { log.Debug(fmt.Sprint(m)) },
	})

	wg.Add(1)
	go func() {
		log.Info("sporkfs started")
		if err := fuseServer.Serve(vfs); err != nil {
			log.Error("serve", zap.Error(err))
		}
		wg.Done()
		cancel()
	}()

	go vfs.WatchInvalidations(ctx, fuseServer)
	go vfs.WatchDeletions(ctx)
}

func parseConfig(dir string) (cfg spork.Config) {
	_, err := toml.DecodeFile(dir, &cfg)
	if err != nil {
		log.Fatal("decoding config", zap.Error(err))
	}
	return
}
