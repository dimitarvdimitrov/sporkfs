package inventory

import (
	"math/rand"
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Driver struct {
	m sync.RWMutex

	root    *store.File
	catalog map[uint64][]*store.File
}

func NewDriver() (Driver, error) {
	rand.Seed(time.Now().UnixNano())

	now := time.Now()
	root := &store.File{
		RWMutex:  &sync.RWMutex{},
		Id:       0,
		Mode:     store.ModeDirectory | 0777,
		Size:     1,
		Version:  0,
		Children: nil,
		Atime:    now,
		Mtime:    now,
	}
	c := make(map[uint64][]*store.File)
	catalogFiles(root, c)

	return Driver{
		root:    root,
		catalog: c,
	}, nil
}

func catalogFiles(root *store.File, catalog map[uint64][]*store.File) {
	catalog[root.Id] = append(catalog[root.Id], root)
	for _, c := range root.Children {
		catalogFiles(c, catalog)
	}
}

func (d Driver) Root() *store.File {
	return d.root
}

func (d Driver) GetAny(id uint64) (*store.File, error) {
	d.m.RLock()
	defer d.m.RUnlock()

	links := d.catalog[id]
	if len(links) == 0 {
		return nil, store.ErrNoSuchFile
	}
	return links[0], nil
}

func (d Driver) GetAll(id uint64) []*store.File {
	d.m.RLock()
	defer d.m.RUnlock()

	return d.catalog[id]
}

func (d Driver) GetSpecific(id, parent uint64, name string) (*store.File, error) {
	d.m.RLock()
	defer d.m.RUnlock()

	for _, link := range d.catalog[id] {
		if ((link.Parent == nil && parent == 0) || (link.Parent != nil && link.Parent.Id == parent)) && link.Name == name {
			return link, nil
		}
	}
	return nil, store.ErrNoSuchFile
}

// SetVersion sets the version for all known links
func (d Driver) SetVersion(id, version uint64) {
	d.m.RLock()
	defer d.m.RUnlock()

	for _, link := range d.catalog[id] {
		link.Version = version
	}
}

// SetVersion sets the version for all known links
func (d Driver) SetSize(id uint64, size int64) {
	d.m.RLock()
	defer d.m.RUnlock()

	for _, link := range d.catalog[id] {
		link.Size = size
	}
}

func (d Driver) Add(f *store.File) {
	d.m.Lock()
	defer d.m.Unlock()

	d.catalog[f.Id] = append(d.catalog[f.Id], f)
}

// Remove deletes the from the inventory and returns true if there are any more hard links to it
func (d Driver) Remove(f *store.File) bool {
	d.m.Lock()
	defer d.m.Unlock()

	foundAt := -1
	for i, link := range d.catalog[f.Id] {
		if link.Name == f.Name && link.Parent.Id == f.Parent.Id {
			foundAt = i
		}
	}
	if foundAt != -1 {
		d.catalog[f.Id] = append(d.catalog[f.Id][:foundAt], d.catalog[f.Id][foundAt+1:]...)
	}

	for i, c := range f.Parent.Children {
		if c.Id == f.Id && c.Name == f.Name {
			f.Parent.Children = append(f.Parent.Children[:i], f.Parent.Children[i+1:]...)
			f.Parent.Size--
			break
		}
	}

	if len(d.catalog[f.Id]) == 0 {
		delete(d.catalog, f.Id)
		return false
	}
	return true
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
