package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/dimitarvdimitrov/sporkfs/api"
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	defer log.Sync()

	flag.Parse()
	mountpoint := flag.Arg(0)
	dataDir := flag.Arg(1)
	thisPeer := flag.Arg(2)

	ctx, cancel := context.WithCancel(context.Background())
	cfg := newSporkConfig(dataDir)
	cfg.ThisPeer = thisPeer

	dataStorage := data.NewLocalDriver(cfg.DataLocation)
	inv := inventory.NewDriver(cfg.InventoryLocation)

	invNodes := make(chan fs.Node)
	invFiles := make(chan *store.File)

	sporkService := spork.New(dataStorage, nil, inv, cfg, invFiles)
	vfs := sfuse.NewFS(&sporkService, invFiles, invNodes)

	startFuseServer(ctx, cancel, mountpoint, vfs, invNodes)
	defer vfs.Destroy()
	startSporkServer(ctx, cancel, thisPeer, dataStorage)
	handleOsSignals(ctx, cancel)
	unmountWhenDone(ctx, mountpoint)

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

func startFuseServer(ctx context.Context, cancel context.CancelFunc, mountpoint string, vfs sfuse.Fs, invalidations <-chan fs.Node) {
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

	go func() {
		for {
			select {
			case n, ok := <-invalidations:
				if !ok {
					return
				}
				_ = fuseServer.InvalidateNodeAttr(n)
				_ = fuseServer.InvalidateNodeData(n)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func startSporkServer(ctx context.Context, cancel context.CancelFunc, listenAddr string, s data.Driver) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	reflection.Register(grpcServer)
	proto.RegisterFileServer(grpcServer, api.NewFileServer(s))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error(err)
		}
		cancel()
	}()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
}

func newSporkConfig(dir string) spork.Config {
	return spork.Config{
		InventoryLocation: fmt.Sprintf("%s/%s", dir, "inventory"),
		DataLocation:      fmt.Sprintf("%s/%s", dir, "data"),
		Peers:             []string{"localhost:8080", "localhost:8081"},
	}
}
