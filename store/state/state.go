package state

import (
	"path"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Driver struct {
	location string
	root     *fileNode
	catalog  map[uint64]*fileNode
}

func NewDriver(location string) Driver {
	root := readFiles(location)
	c := make(map[uint64]*fileNode)
	catalogFiles(root, c)

	return Driver{
		location: location,
		root:     root,
		catalog:  c,
	}
}

func catalogFiles(root *fileNode, catalog map[uint64]*fileNode) {
	catalog[root.Id] = root
	for _, c := range root.children {
		catalogFiles(c, catalog)
	}
}

func (d Driver) Root() *store.File {
	return d.root.File
}

func (d Driver) ReadDir(path string) ([]*store.File, error) {
	nodes, found := readDir(d.root, path)
	if !found {
		return nil, store.ErrNoSuchFile
	}

	return convertNodes(nodes), nil
}

// TODO maybe refactor to have a function read() that takes a root and a path and return the node at the path
// readDir walks from the root to find the path and return everything there
// and a bool that indicating if the path was found
func readDir(root *fileNode, p string) ([]*fileNode, bool) {
	if root.Name != rootname(p) {
		return nil, false // no such dir
	}

	if dir, file := path.Split(p); dir == "" && file == root.Name {
		return root.children, true
	}

	nextPath := stripRootSegment(p)
	for _, c := range root.children {
		if result, _ := readDir(c, nextPath); result != nil {
			return result, true
		}
	}
	return nil, false
}

func (d Driver) ChildrenOf(id uint64) []*store.File {
	if node, ok := d.catalog[id]; ok {
		return convertNodes(node.children)
	}
	return nil
}
