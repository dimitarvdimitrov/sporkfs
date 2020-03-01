package spork

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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

	commitC <-chan raft.UnactionedMessage
	wg      *sync.WaitGroup
}

func New(ctx context.Context, cancel context.CancelFunc, cfg Config, invalid, deleted chan<- *store.File) (Spork, error) {
	data, err := storedata.NewLocalDriver(cfg.DataDir + "/data")
	if err != nil {
		return Spork{}, fmt.Errorf("init data driver: %s", err)
	}
	cacheData, err := storedata.NewLocalDriver(cfg.DataDir + "/cache")
	if err != nil {
		return Spork{}, fmt.Errorf("init data driver: %s", err)
	}
	c := cache.New(cacheData)

	inv, err := inventory.NewDriver()
	if err != nil {
		return Spork{}, fmt.Errorf("init inventory: %s", err)
	}

	r, commits, peers := raft.New(cfg.Config, inv)
	fetcher, err := remote.NewFetcher(peers)
	if err != nil {
		return Spork{}, fmt.Errorf("init fetcher: %s", err)
	}

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
		wg:        &sync.WaitGroup{},
	}
	startGrpcServer(ctx, cancel, cfg.Config.ThisPeer, data, c, r, s.wg)
	s.wg.Add(1)
	go s.watchRaft()

	return s, nil
}

func startGrpcServer(ctx context.Context, cancel context.CancelFunc, listenAddr string, data, cache storedata.Driver, raft *raft.Raft, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	grpcServer := grpc.NewServer()

	reflection.Register(grpcServer)
	proto.RegisterFileServer(grpcServer, api.NewFileServer(data, cache))
	raftpb.RegisterRaftServer(grpcServer, raft)

	wg.Add(1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("serve grpc", zap.Error(err))
		}
		cancel()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		<-ctx.Done()
		time.AfterFunc(time.Second*10, grpcServer.Stop) // just nuke it if it doesn't give up
		grpcServer.GracefulStop()
		wg.Done()
	}()
}

func (s Spork) Root() *store.File {
	return s.inventory.Root()
}

func (s Spork) Lookup(f *store.File, name string) (*store.File, error) {
	f.RLock()
	defer f.RUnlock()

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
	log.Debug("opening file for read/write", log.Id(f.Id), log.Ver(f.Version))

	driver, err := s.ensureFile(f)
	if err != nil {
		return nil, err
	}

	nextVersion := rand.Uint64()

	r, w, err := driver.Open(f.Id, f.Version, nextVersion, flags)
	if err != nil {
		return nil, err
	}

	rw := &readWriter{
		r: &reader{
			f: f,
			r: r,
		},
		w: &writer{
			written:         flags&os.O_TRUNC != 0 || (flags&os.O_APPEND == 0 && flags&os.O_TRUNC == 0),
			startingVersion: f.Version,
			endingVersion:   nextVersion,
			f:               f,
			fileSizer:       driver,
			fileRemover:     driver,
			w:               w,
			invalidate:      s.invalid,
			changer:         s.raft,
			links:           s.inventory,
		},
	}
	return rw, nil
}

func (s Spork) Read(f *store.File, flags int) (ReadCloser, error) {
	f.RLock()
	defer f.RUnlock()

	driver, err := s.ensureFile(f)
	if err != nil {
		return nil, err
	}

	r, err := driver.Reader(f.Id, f.Version, flags)
	if err != nil {
		return nil, err
	}

	reader := &reader{
		f: f,
		r: r,
	}
	return reader, nil
}

// ensureFile makes sure the file is present locally and returns the driver from which the file can be read
func (s Spork) ensureFile(f *store.File) (storedata.Driver, error) {
	driver := s.data
	if !s.peers.IsLocalFile(f.Id) {
		driver = s.cache
	}

	err := s.maybeTransferRemoteFile(f.Id, f.Version, driver)
	if err != nil {
		return nil, err
	}

	return driver, nil
}

func (s Spork) maybeTransferRemoteFile(id, version uint64, dst storedata.Driver) error {
	if dst.Contains(id, version) {
		log.Debug("file already present in destination", log.Id(id), log.Ver(version))
		return nil
	}
	log.Debug("transferring remote file", log.Id(id), log.Ver(version))

	r, err := s.fetcher.Reader(id, version)
	if err != nil {
		return fmt.Errorf("initing fetcher for id:%d, version:%d, err:%w", id, version, err)
	}
	defer r.Close()

	w, err := dst.Writer(id, version, version, os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("writing file to destination id:%d, version:%d, err:%w", id, version, err)
	}
	defer w.Commit()

	_, err = io.Copy(w, r)
	if err != nil {
		dst.Remove(id, version) // remove so that we can retry next time
		return fmt.Errorf("error during stream transfer of id:%d, version:%d, err:%w", id, version, err)
	}
	return nil
}

func (s Spork) updateLocalFile(id, oldVersion, newVersion uint64, peerHint string, dst storedata.Driver) error {
	log.Debug("transferring remote file", log.Id(id), log.Ver(newVersion), zap.Uint64("old_version", oldVersion))
	if dst.Contains(id, newVersion) {
		log.Debug("[spork] skipping transfer since file is already here",
			log.Id(id),
			log.Ver(newVersion),
			zap.String("peer", peerHint),
		)
		return nil
	}

	r, err := s.fetcher.ReaderFromPeer(id, newVersion, peerHint)
	if err != nil {
		r, err = s.fetcher.Reader(id, newVersion)
		if err != nil {
			return err
		}
	}
	defer r.Close()

	w, err := dst.Writer(id, oldVersion, newVersion, os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer w.Commit()

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

	for _, c := range parent.Children {
		if c.Name == name {
			return nil, store.ErrFileAlreadyExists
		}
	}

	f := s.newFile(name, mode)
	f.Lock()
	defer f.Unlock()

	committed, callback := s.raft.Add(f.Id, parent.Id, f.Name, f.Mode)
	if !committed {
		return nil, fmt.Errorf("failed to add file in raft")
	}
	defer callback()

	s.add(f, parent)

	return f, nil
}

func (s Spork) CreateLink(file, parent *store.File, linkName string) (*store.File, error) {
	parent.Lock()
	defer parent.Unlock()

	for _, c := range parent.Children {
		if c.Name == linkName {
			return nil, store.ErrFileAlreadyExists
		}
	}

	link := s.newFile(linkName, file.Mode)
	link.Lock()
	defer link.Unlock()

	link.RWMutex = file.RWMutex
	link.Id = file.Id
	link.Version = file.Version
	link.Mode = file.Mode
	link.Atime = file.Atime
	link.Mtime = file.Mtime
	link.Size = file.Size

	committed, callback := s.raft.Add(link.Id, parent.Id, link.Name, link.Mode)
	if !committed {
		return nil, fmt.Errorf("failed to add file in raft")
	}
	defer callback()

	s.add(link, parent)

	return link, nil
}

func (s Spork) add(file *store.File, parent *store.File) {
	file.Parent = parent
	s.inventory.Add(file)
	parent.Children = append(parent.Children, file)
	parent.Size = int64(len(parent.Children))
}

func (s Spork) newFile(name string, mode store.FileMode) *store.File {
	now := time.Now()
	return &store.File{
		RWMutex:  &sync.RWMutex{},
		Id:       s.inventory.NewId(),
		Version:  0,
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

	committed, callback := s.raft.Rename(file.Id, oldParent.Id, newParent.Id, file.Name, newName)
	if !committed {
		return fmt.Errorf("couldn't vote raft change")
	}
	defer callback()

	s.invalid <- file
	time.Sleep(time.Microsecond)
	s.rename(file, newParent, oldParent, newName)

	return nil
}

func (s Spork) rename(file *store.File, newParent *store.File, oldParent *store.File, newName string) {
	file.Name = newName

	if oldParent.Id != newParent.Id {
		for i, c := range oldParent.Children {
			if c.Id == file.Id && c.Name == file.Name {
				oldParent.Children = append(oldParent.Children[:i], oldParent.Children[i+1:]...)
				oldParent.Size--
				break
			}
		}

		file.Parent = newParent
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

	committed, callback := s.raft.Delete(file.Id, parent.Id, file.Name)
	if !committed {
		return fmt.Errorf("couldn't vote removal in raft")
	}
	defer callback()

	s.delete(file)

	return nil
}

func (s Spork) delete(file *store.File) {
	if !s.inventory.Remove(file) {
		s.data.Remove(file.Id, file.Version)
		s.cache.Remove(file.Id, file.Version)
	}
}

func (s Spork) Close() {
	log.Info("stopping spork...")
	s.raft.Shutdown()
	s.wg.Wait()
	close(s.invalid)
	close(s.deleted)
	log.Info("stopped spork")
}
