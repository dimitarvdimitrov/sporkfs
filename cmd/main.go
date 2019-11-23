package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/api"
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	sfuse "github.com/dimitarvdimitrov/sporkfs/fuse"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
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

	done := make(chan struct{}, 2)
	sporkService := spork.New(newSporkConfig(dataDir))
	vfs := sfuse.Fs{S: sporkService}

	startFuseServer(mountpoint, vfs, done)
	defer vfs.Destroy()

	grpcServer := startSporkServer("localhost:8080", sporkService, done)
	defer grpcServer.GracefulStop()

	unmountOnOsSignals(mountpoint, done)

	<-done
	log.Info("stopping spork...")
}

func unmountOnOsSignals(mountpoint string, done chan struct{}) {
	go func() {
		signals := make(chan os.Signal)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals

		if err := syscall.Unmount(mountpoint, 0); err != nil {
			log.Errorf("unmount: %s", err)
		}

		done <- struct{}{}
	}()
}

func startFuseServer(mountpoint string, vfs sfuse.Fs, done chan struct{}) {
	log.Infof("mounting sporkfs at %s...", mountpoint)
	fuseConn, err := fuse.Mount(mountpoint,
		fuse.FSName("sporkfs"),
		fuse.VolumeName("sporkfs"),
	)
	if err != nil {
		log.Fatal("couldn't mount: ", err)
	}

	fuseServer := fs.New(fuseConn, &fs.Config{
		Debug: func(m interface{}) { log.Debug(m) },
	})

	log.Infof("mount successful")

	go func() {
		log.Info("sporkfs started")
		if err := fuseServer.Serve(vfs); err != nil {
			log.Error("serve: ", err)
		}
		done <- struct{}{}
	}()
}

func startSporkServer(listenAddr string, s spork.Spork, done chan struct{}) *grpc.Server {
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
		done <- struct{}{}
	}()

	return grpcServer
}

func newSporkConfig(dir string) spork.Config {
	return spork.Config{
		InventoryLocation: fmt.Sprintf("%s/%s", dir, "inventory"),
		DataLocation:      fmt.Sprintf("%s/%s", dir, "data"),
	}
}
