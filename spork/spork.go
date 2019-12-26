package spork

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/raft"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
	"github.com/dimitarvdimitrov/sporkfs/store/remote"
)

type Spork struct {
	inventory   inventory.Driver
	data, cache data.Driver
	invalid     chan<- *store.File

	peers   *raft.Peers
	raft    *raft.Raft
	fetcher remote.Readerer

	commitC <-chan *raftpb.Entry
}

func New(data, cache data.Driver,
	inv inventory.Driver,
	fetcher remote.Readerer,
	peers *raft.Peers,
	invalid chan<- *store.File,
	r *raft.Raft,
	commits <-chan *raftpb.Entry) Spork {

	s := Spork{
		inventory: inv,
		data:      data,
		cache:     cache,
		fetcher:   fetcher,
		peers:     peers,
		raft:      r,
		commitC:   commits,
		invalid:   invalid,
	}

	go s.watchRaft()
	return s
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
	r, w, err := s.data.Open(f.Id, f.Hash, flags)
	if err != nil {
		return nil, err
	}

	rw := &readWriter{
		r: &reader{
			f: f,
			r: r,
		},
		w: &writer{
			f:          f,
			fileSizer:  s.data,
			w:          w,
			invalidate: s.invalid,
			r:          s.raft,
		},
	}
	return rw, nil
}

func (s Spork) Read(f *store.File, flags int) (ReadCloser, error) {
	return s.ReadVersion(f, f.Hash, flags)
}

func (s Spork) ReadVersion(f *store.File, version uint64, flags int) (ReadCloser, error) {
	driver := s.data

	if !s.peers.IsLocalFile(f.Id) {
		log.Debugf("reading remote file")
		err := s.transferRemoteFile(f.Id, version, s.cache)
		if err != nil {
			return nil, err
		}
		driver = s.cache
	}

	r, err := driver.Reader(f.Id, version, flags)
	if err != nil {
		return nil, err
	}

	return &reader{
		f: f,
		r: r,
	}, nil
}

func (s Spork) transferRemoteFile(id, version uint64, dst data.Driver) error {
	log.Debugf("transferring remote file %d-%d", id, version)
	if dst.Contains(id, version) {
		return nil
	}

	// TODO this only works for when the file isn't present locally. any further changes from raft error here
	v, err := dst.Add(id, store.ModeRegularFile)
	if err != nil {
		return err
	}

	w, err := dst.Writer(id, v, os.O_WRONLY)
	if err != nil {
		return err
	}
	defer w.Close()

	r, err := s.fetcher.Reader(id, version)
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
	driver := s.data

	if !s.peers.IsLocalFile(f.Id) {
		err := s.transferRemoteFile(f.Id, f.Hash, s.cache)
		if err != nil {
			return nil, err
		}
		driver = s.cache
	}

	w, err := driver.Writer(f.Id, f.Hash, flags)
	if err != nil {
		return nil, err
	}

	return &writer{
		w:          w,
		f:          f,
		r:          s.raft,
		fileSizer:  s.data,
		invalidate: s.invalid,
	}, nil
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

	err := s.createLocally(f, parent)
	if err != nil {
		s.raft.Delete(f.Id, parent.Id)
		return nil, err
	}
	return f, nil
}

func (s Spork) createLocally(f, parent *store.File) error {
	err := s.inventory.Add(f)
	if err != nil {
		return err
	}

	hash, err := s.data.Add(f.Id, f.Mode)
	if err != nil {
		s.inventory.Remove(f.Id)
		return err
	}
	f.Hash = hash

	parent.Children = append(parent.Children, f)
	parent.Size = int64(len(parent.Children))

	return nil
}

func (s Spork) newFile(name string, mode store.FileMode) *store.File {
	return &store.File{
		RWMutex:  &sync.RWMutex{},
		Id:       s.inventory.NewId(),
		Name:     name,
		Mode:     mode,
		Size:     0,
		Children: nil,
	}
}

func (s Spork) Rename(file, oldParent, newParent *store.File, newName string) error {
	if !s.raft.Rename(file.Id, oldParent.Id, newParent.Id, newName) {
		return fmt.Errorf("couldn't vote raft change")
	}

	s.renameLocally(file, newParent, oldParent, newName)
	return nil
}

func (s Spork) renameLocally(file *store.File, newParent *store.File, oldParent *store.File, newName string) {
	oldParent.Lock()
	defer oldParent.Unlock()

	file.Lock()
	defer file.Unlock()

	file.Name = newName

	if oldParent.Id != newParent.Id {
		newParent.Lock()
		defer newParent.Unlock()

		for i, c := range oldParent.Children {
			if c.Id == file.Id {
				oldParent.Children = append(oldParent.Children[:i], oldParent.Children[i+1:]...)
				break
			}
		}

		newParent.Children = append(newParent.Children, file)
	}

}

func (s Spork) Delete(file, parent *store.File) error {
	if len(file.Children) != 0 {
		return store.ErrDirectoryNotEmpty
	}

	file.Lock()
	defer file.Unlock()

	parent.Lock()
	defer parent.Unlock()

	index := -1
	for i, c := range parent.Children {
		if c.Id == file.Id {
			index = i
			break
		}
	}

	if index == -1 {
		return store.ErrNoSuchFile
	}

	if !s.raft.Delete(file.Id, parent.Id) {
		return fmt.Errorf("couldn't vote removal in raft")
	}

	s.deleteLocally(file, parent, index)

	return nil
}

func (s Spork) deleteLocally(file *store.File, parent *store.File, index int) {
	s.data.Remove(file.Id, file.Hash)
	s.inventory.Remove(file.Id)
	parent.Children = append(parent.Children[:index], parent.Children[index+1:]...)
	parent.Size--
}

func (s Spork) Close() {
	close(s.invalid)
	s.raft.Shutdown()
	s.data.Sync()
	s.inventory.Sync()
}
