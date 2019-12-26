package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"

	"github.com/BurntSushi/toml"
	"github.com/dimitarvdimitrov/sporkfs/api"
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/raft"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
	"github.com/dimitarvdimitrov/sporkfs/store/remote"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	defer log.Sync()

	flag.Parse()
	cfgLocation := flag.Arg(0)

	cfg := parseConfig(cfgLocation)

	ctx, cancel := context.WithCancel(context.Background())
	peers := raft.NewPeerList(cfg.Raft)

	dataStorage, err := data.NewLocalDriver(cfg.DataDir + "/data")
	if err != nil {
		log.Fatalf("init data driver: %s", err)
	}
	cacheStorage, err := data.NewLocalDriver(cfg.DataDir + "/cache")
	if err != nil {
		log.Fatalf("init data driver: %s", err)
	}

	inv, err := inventory.NewDriver(cfg.DataDir + "/inventory")
	if err != nil {
		log.Fatalf("init inventory: %s", err)
	}

	fetcher, err := remote.NewFetcher(peers)
	if err != nil {
		log.Fatalf("init fetcher: %s", err)
	}

	invFiles := make(chan *store.File)
	r, commits := raft.New(peers) // TODO remove this from here; move the raft init and the starting of grpc server inside spork.New
	sporkService := spork.New(dataStorage, cacheStorage, inv, fetcher, peers, invFiles, r, commits)
	vfs := sfuse.NewFS(&sporkService, invFiles)

	startFuseServer(ctx, cancel, cfg.MountPoint, vfs)
	defer vfs.Destroy()
	startGrpcServer(ctx, cancel, cfg.Raft.ThisPeer, dataStorage, r)
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

func startGrpcServer(ctx context.Context, cancel context.CancelFunc, listenAddr string, data data.Driver, raft *raft.Raft) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	reflection.Register(grpcServer)
	proto.RegisterFileServer(grpcServer, api.NewFileServer(data))
	raftpb.RegisterRaftServer(grpcServer, raft)

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

func parseConfig(dir string) (cfg spork.Config) {
	_, err := toml.DecodeFile(dir, &cfg)
	if err != nil {
		log.Fatalf("decoding config: %s", err)
	}
	return
}
