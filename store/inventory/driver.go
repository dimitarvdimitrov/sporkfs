package inventory

import (
	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Driver struct {
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
