package spork

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/api"
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/raft"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
	storedata "github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/data/cache"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
	"github.com/dimitarvdimitrov/sporkfs/store/remote"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Spork struct {
	inventory        inventory.Driver
	data             storedata.Driver
	cache            cache.Cache
	invalid, deleted chan<- *store.File

	peers   *raft.Peers
	raft    *raft.Raft
	fetcher remote.Readerer

	commitC <-chan *raftpb.Entry
}

func New(ctx context.Context, cancel context.CancelFunc, cfg Config, invalid, deleted chan<- *store.File) (Spork, error) {
	peers := raft.NewPeerList(cfg.Raft)

	data, err := storedata.NewLocalDriver(cfg.DataDir + "/data")
	if err != nil {
		return Spork{}, fmt.Errorf("init data driver: %s", err)
	}
	cacheData, err := storedata.NewLocalDriver(cfg.DataDir + "/cache")
	if err != nil {
		return Spork{}, fmt.Errorf("init data driver: %s", err)
	}
	c := cache.New(cacheData)

	inv, err := inventory.NewDriver(cfg.DataDir + "/inventory")
	if err != nil {
		return Spork{}, fmt.Errorf("init inventory: %s", err)
	}

	fetcher, err := remote.NewFetcher(peers)
	if err != nil {
		return Spork{}, fmt.Errorf("init fetcher: %s", err)
	}

	r, commits := raft.New(peers)

	s := Spork{
		inventory: inv,
		data:      data,
		cache:     c,
		fetcher:   fetcher,
		peers:     peers,
		raft:      r,
		commitC:   commits,
		invalid:   invalid,
		deleted:   deleted,
	}

	startGrpcServer(ctx, cancel, cfg.Raft.ThisPeer, data, c, r)
	go s.watchRaft()

	return s, nil
}

func startGrpcServer(ctx context.Context, cancel context.CancelFunc, listenAddr string, data, cache storedata.Driver, raft *raft.Raft) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	grpcServer := grpc.NewServer()

	reflection.Register(grpcServer)
	proto.RegisterFileServer(grpcServer, api.NewFileServer(data, cache))
	raftpb.RegisterRaftServer(grpcServer, raft)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("serve grpc", zap.Error(err))
		}
		cancel()
	}()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
}

func (s Spork) Root() *store.File {
	return s.inventory.Root()
}

func (s Spork) Lookup(f *store.File, name string) (*store.File, error) {
	for _, c := range f.Children {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, store.ErrNoSuchFile
}

func (s Spork) ReadWriter(f *store.File, flags int) (ReadWriteCloser, error) {
	f.Lock()
	defer f.Unlock()
	log.Debug("opening file for read/write", log.Id(f.Id), log.Hash(f.Hash))

	driver := s.data
	if !s.peers.IsLocalFile(f.Id) {
		log.Debug("reading remote file for read/write")
		err := s.transferRemoteFile(f.Id, f.Hash, s.cache)
		if err != nil {
			return nil, err
		}
		driver = s.cache
	}

	r, w, err := driver.Open(f.Id, f.Hash, flags)
	if err != nil {
		return nil, err
	}

	rw := &readWriter{
		r: &reader{
			f: f,
			r: r,
		},
		w: &writer{
			startingHash: f.Hash,
			f:            f,
			fileSizer:    driver,
			fileRemover:  driver,
			w:            w,
			invalidate:   s.invalid,
			changer:      s.raft,
		},
	}
	return rw, nil
}

func (s Spork) Read(f *store.File, flags int) (ReadCloser, error) {
	f.Lock()
	defer f.Unlock()

	driver := s.data
	if !s.peers.IsLocalFile(f.Id) {
		log.Debug("reading remote file for read")
		err := s.transferRemoteFile(f.Id, f.Hash, s.cache)
		if err != nil {
			return nil, err
		}
		driver = s.cache
	}

	r, err := driver.Reader(f.Id, f.Hash, flags)
	if err != nil {
		return nil, err
	}

	reader := &reader{
		f: f,
		r: r,
	}
	return reader, nil
}

func (s Spork) transferRemoteFile(id, version uint64, dst storedata.Driver) error {
	log.Debug("transferring remote file", log.Id(id), log.Hash(version))
	if dst.Contains(id, version) {
		log.Debug("file already present in destination", log.Id(id), log.Hash(version))
		return nil
	}

	w, err := dst.Writer(id, 0, os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("writing file to destination id:%d, hash:%d, err:%w", id, version, err)
	}
	defer w.Close()

	r, err := s.fetcher.Reader(id, version)
	if err != nil {
		return fmt.Errorf("initing fetcher for id:%d, hash:%d, err:%w", id, version, err)
	}
	defer r.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("error during stream transfer of id:%d, hash: %d, err:%w", id, version, err)
	}
	return nil
}

func (s Spork) updateLocalFile(id, oldVersion, newVersion uint64, peer string, dst storedata.Driver) error {
	log.Debug("transferring remote file", log.Id(id), log.Hash(newVersion))
	if dst.Contains(id, newVersion) {
		return nil
	}

	w, err := dst.Writer(id, oldVersion, os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer w.Close()

	r, err := s.fetcher.ReaderFromPeer(id, newVersion, peer)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	return nil
}

func (s Spork) Write(f *store.File, flags int) (WriteCloser, error) {
	return s.ReadWriter(f, flags)
}

func (s Spork) CreateFile(parent *store.File, name string, mode store.FileMode) (*store.File, error) {
	parent.Lock()
	defer parent.Unlock()

	f := s.newFile(name, mode)
	f.Lock()
	defer f.Unlock()

	if !s.raft.Add(f.Id, parent.Id, f.Name, f.Mode) {
		return nil, fmt.Errorf("failed to add file in raft")
	}

	err := s.inventory.Add(f)
	if err != nil {
		log.Error("creating file locally", zap.Error(err))
		return nil, err
	}

	err = s.createInCacheOrData(f, parent)
	if err != nil {
		log.Error("creating file locally", zap.Error(err))
		return nil, err
	}

	parent.Children = append(parent.Children, f)
	parent.Size = int64(len(parent.Children))

	return f, nil
}

func (s Spork) createInCacheOrData(f, parent *store.File) error {
	driver := s.data
	if !s.peers.IsLocalFile(f.Id) {
		driver = s.cache
	}

	hash, err := driver.Add(f.Id, f.Mode)
	if err != nil {
		return err
	}
	f.Hash = hash

	return nil
}

func (s Spork) newFile(name string, mode store.FileMode) *store.File {
	now := time.Now()
	return &store.File{
		RWMutex:  &sync.RWMutex{},
		Id:       s.inventory.NewId(),
		Name:     name,
		Mode:     mode,
		Size:     0,
		Children: nil,
		Mtime:    now,
		Atime:    now,
	}
}

func (s Spork) Rename(file, oldParent, newParent *store.File, newName string) error {
	oldParent.Lock()
	defer oldParent.Unlock()

	file.Lock()
	defer file.Unlock()

	if oldParent.Id != newParent.Id {
		newParent.Lock()
		defer newParent.Unlock()
	}

	if !s.raft.Rename(file.Id, oldParent.Id, newParent.Id, newName) {
		return fmt.Errorf("couldn't vote raft change")
	}

	s.renameLocally(file, newParent, oldParent, newName)
	return nil
}

func (s Spork) renameLocally(file *store.File, newParent *store.File, oldParent *store.File, newName string) {
	file.Name = newName

	if oldParent.Id != newParent.Id {
		for i, c := range oldParent.Children {
			if c.Id == file.Id {
				oldParent.Children = append(oldParent.Children[:i], oldParent.Children[i+1:]...)
				oldParent.Size--
				break
			}
		}

		newParent.Children = append(newParent.Children, file)
		newParent.Size++
	}
}

func (s Spork) Delete(file *store.File) error {
	if len(file.Children) != 0 {
		return store.ErrDirectoryNotEmpty
	}

	file.Lock()
	defer file.Unlock()

	parent := file.Parent
	parent.Lock()
	defer parent.Unlock()

	found := false
	for _, c := range parent.Children {
		if c.Id == file.Id {
			found = true
			break
		}
	}

	if !found {
		return store.ErrNoSuchFile
	}

	if !s.raft.Delete(file.Id, parent.Id) {
		return fmt.Errorf("couldn't vote removal in raft")
	}

	s.deleteLocally(file)

	return nil
}

func (s Spork) deleteLocally(file *store.File) {
	s.data.Remove(file.Id, file.Hash)
	s.cache.Remove(file.Id, file.Hash)
	s.inventory.Remove(file.Id)

	for i, c := range file.Parent.Children {
		if c.Id == file.Id {
			file.Parent.Children = append(file.Parent.Children[:i], file.Parent.Children[i+1:]...)
			file.Parent.Size--
			break
		}
	}
}

func (s Spork) Close() {
	close(s.invalid)
	s.raft.Shutdown()
	s.data.Sync()
	s.inventory.Sync()
}
