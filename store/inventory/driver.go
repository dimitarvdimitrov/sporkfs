package inventory

import (
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Driver struct {
	m sync.RWMutex

	location string
	root     *store.File
	catalog  map[uint64]*store.File
}

func NewDriver(location string) (Driver, error) {
	if err := os.MkdirAll(location, os.ModeDir|0755); err != nil {
		return Driver{}, err
	}
	now := time.Now()
	root := &store.File{
		RWMutex:  &sync.RWMutex{},
		Id:       0,
		Mode:     store.ModeDirectory | 0777,
		Size:     1,
		Hash:     0,
		Children: nil,
		Atime:    now,
		Mtime:    now,
	}
	c := make(map[uint64]*store.File)
	catalogFiles(root, c)

	return Driver{
		location: location,
		root:     root,
		catalog:  c,
	}, nil
}

func catalogFiles(root *store.File, catalog map[uint64]*store.File) {
	catalog[root.Id] = root
	for _, c := range root.Children {
		catalogFiles(c, catalog)
	}
}

func (d Driver) Root() *store.File {
	return d.root
}

func (d Driver) Get(id uint64) (*store.File, error) {
	d.m.RLock()
	defer d.m.RUnlock()

	file, exists := d.catalog[id]
	if !exists {
		return nil, store.ErrNoSuchFile
	}
	return file, nil
}

func (d Driver) Add(f *store.File) {
	d.m.Lock()
	defer d.m.Unlock()

	d.catalog[f.Id] = f
}

func (d Driver) Remove(id uint64) {
	d.m.Lock()
	defer d.m.Unlock()

	delete(d.catalog, id)
}

// NewId returns a new ID. It guarantees that at the time of creation this ID is unique among all files.
func (d Driver) NewId() (id uint64) {
	d.m.RLock()
	defer d.m.RUnlock()

	for {
		id = rand.Uint64()

		if _, exists := d.catalog[id]; !exists {
			return
		}
	}
}
