package inventory

import (
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Driver struct {
	m sync.RWMutex

	location string
	root     *store.File
	catalog  map[uint64]*store.File
}

func NewDriver(location string) Driver {
	root := readFiles(location)
	c := make(map[uint64]*store.File)
	catalogFiles(root, c)

	return Driver{
		location: location,
		root:     root,
		catalog:  c,
	}
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

func (d Driver) Add(f *store.File) error {
	d.m.Lock()
	defer d.m.Unlock()

	if _, ok := d.catalog[f.Id]; ok {
		return store.ErrFileAlreadyExists
	}
	d.catalog[f.Id] = f
	return nil
}

func (d Driver) Remove(id uint64) {
	d.m.Lock()
	defer d.m.Unlock()

	delete(d.catalog, id)
}
