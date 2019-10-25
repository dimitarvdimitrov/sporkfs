package spork

import (
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
)

var S = Spork{
	inventory: inventory.NewDriver(""),
	data:      data.NewLocalDriver("/opt/storage/data"),
}

type Spork struct {
	inventory   inventory.Driver
	data, cache data.Driver
}

func (s Spork) Root() *store.File {
	s.data.Sync() // TODO remove this
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

func (s Spork) ReadAll(f *store.File) ([]byte, error) {
	f.RLock()
	defer f.RUnlock()

	return s.Read(f, 0, f.Size)
}

func (s Spork) Read(f *store.File, offset, size uint64) ([]byte, error) {
	f.RLock()
	defer f.RUnlock()

	return s.data.Read(f, offset, size)
}

func (s Spork) Write(f *store.File, offset int64, data []byte, flags int) (int, error) {
	f.Lock()
	defer f.Unlock()
	defer func() {
		f.Size = uint64(s.data.Size(f))
	}()

	return s.data.Write(f, offset, data, flags)
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

	err = s.data.Add(f)
	if err != nil {
		s.inventory.Remove(f.Id)
		return nil, err
	}

	parent.Children = append(parent.Children, f)
	return f, nil
}

func (s Spork) newFile(name string, mode store.FileMode) *store.File {
	return &store.File{
		Id:       uint64(time.Now().Nanosecond()),
		Name:     name,
		Mode:     mode,
		Size:     0,
		Children: nil,
	}
}

func (s Spork) Close() {
	s.data.Sync()
}
