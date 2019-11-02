package inventory

import (
	"encoding/binary"
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/google/uuid"
)

type Driver struct {
	m sync.RWMutex

	location string
	root     *store.File
	catalog  map[uint64]*store.File
}

func NewDriver(location string) Driver {
	root := restoreInventory(location + indexLocation)
	c := make(map[uint64]*store.File)
	catalogFiles(root, c)
	initLocks(root, c)

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

func initLocks(root *store.File, catalog map[uint64]*store.File) {
	if existingRoot, ok := catalog[root.Id]; ok && existingRoot.RWMutex != nil {
		root.RWMutex = existingRoot.RWMutex
	} else {
		root.RWMutex = &sync.RWMutex{}
	}
	for _, c := range root.Children {
		initLocks(c, catalog)
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

// NewId returns a new ID. It guarantees that at the time of creation this ID is unique among all files.
func (d Driver) NewId() uint64 {
	d.m.RLock()
	defer d.m.RUnlock()

	for {
		uuidBytes, _ := uuid.New().MarshalBinary()
		id := binary.BigEndian.Uint64(uuidBytes[:])

		if _, exists := d.catalog[id]; exists {
			continue
		}
		return id
	}
}
