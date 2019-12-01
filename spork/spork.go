package spork

import (
	"bytes"
	"io"
	"sort"
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/raft/index"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
)

type Spork struct {
	inventory   inventory.Driver
	data, cache data.Driver
	fetcher     data.Readerer

	cfg Config
}

func New(data, cache data.Driver, inv inventory.Driver, cfg Config) Spork {
	sort.Strings(cfg.Peers)
	return Spork{
		inventory: inv,
		data:      data,
		cache:     cache,
		cfg:       cfg,
	}
}

func (s Spork) Root() *store.File {
	return s.inventory.Root()
}

func (s Spork) Get(id uint64) (*store.File, error) {
	return s.inventory.Get(id)
}

func (s Spork) Lookup(f *store.File, name string) (*store.File, error) {
	for _, c := range f.Children {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, store.ErrNoSuchFile
}

func (s Spork) Read(f *store.File, offset, size int64) ([]byte, error) {
	return s.ReadVersion(f, f.Hash, offset, size)
}

func (s Spork) ReadVersion(f *store.File, version uint64, offset, size int64) ([]byte, error) {
	f.RLock()
	defer f.RUnlock()

	var driver = s.data
	if false {
		driver = s.cache
		if !driver.Contains(f.Id, version) {
			s.fetcher.Reader(f.Id, version, offset, size)
		}
	}

	result := make([]byte, 0, size)
	var rBuff = bytes.NewBuffer(result)
	content, err := driver.Reader(f.Id, version, offset, size)
	if err != nil && err != io.EOF {
		return nil, err
	}
	n, err := rBuff.ReadFrom(content)

	return result[:n], err
}

func (s Spork) fileShouldBeCached(id uint64) bool {
	peersWithFile := index.FindPeers(s.cfg.Peers, id)
	for _, p := range peersWithFile {
		if p == s.cfg.ThisPeer {
			return false
		}
	}
	return true
}

func (s Spork) Write(f *store.File, offset int64, data []byte, flags int) (int, error) {
	f.Lock()
	defer f.Unlock()
	defer func() {
		f.Size = s.data.Size(f.Id, f.Hash)
	}()

	writer, getNewHash, err := s.data.Writer(f.Id, f.Hash, offset, flags)
	if err != nil {
		return 0, err
	}
	defer writer.Close()

	var wBuff io.Reader = bytes.NewBuffer(data)
	written, err := io.Copy(writer, wBuff)
	if err != nil {
		return int(written), err
	}
	_ = writer.Close() // we need Close() so that getNewHash is available

	f.Hash = getNewHash()

	return int(written), nil
}

func (s Spork) CreateFile(parent *store.File, name string, mode store.FileMode) (*store.File, error) {
	parent.Lock()
	defer parent.Unlock()

	f := s.newFile(name, mode)
	f.Lock()
	defer f.Unlock()

	err := s.inventory.Add(f)
	if err != nil {
		return nil, err
	}

	hash, err := s.data.Add(f.Id, f.Mode)
	if err != nil {
		s.inventory.Remove(f.Id)
		return nil, err
	}
	f.Hash = hash

	parent.Children = append(parent.Children, f)
	parent.Size = int64(len(parent.Children))

	return f, nil
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

	return nil
}

func (s Spork) Delete(file, parent *store.File) error {
	if len(file.Children) != 0 {
		return store.ErrDirectoryNotEmpty
	}

	file.Lock()
	defer file.Unlock()

	parent.Lock()
	defer parent.Unlock()

	found := false
	for i, c := range parent.Children {
		if c.Id == file.Id {
			s.data.Remove(file.Id, file.Hash)
			s.inventory.Remove(file.Id)
			parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
			parent.Size--

			found = true
			break
		}
	}

	if !found {
		return store.ErrNoSuchFile
	}

	return nil
}

//func (s Spork) Link(file, newParent *store.File, name string) *store.File {
//	file.RLock()
//	defer file.RUnlock()
//
//	newParent.Lock()
//	defer newParent.Unlock()
//
//	newFile := file.Copy()
//	newFile.Name = name
//	newParent.Children = append(newParent.Children, newFile)
//
//	return newFile
//}

func (s Spork) Close() {
	s.data.Sync()
	s.inventory.Sync()
}
