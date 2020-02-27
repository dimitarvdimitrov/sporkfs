package fuse

import (
	"sync"
)

type nodeRegistrar interface {
	registerNode(node)
	deleteNode(node)
	nodeRegistered(node) bool
	getNode(uint64, uint64, string) (node, bool)
}

type registrar struct {
	*sync.RWMutex

	registeredNodes map[uint64][]node
}

func (r *registrar) registerNode(n node) {
	r.Lock()
	defer r.Unlock()
	r.registeredNodes[n.Id] = append(r.registeredNodes[n.Id], n)
}

func (r *registrar) deleteNode(f node) {
	r.Lock()
	defer r.Unlock()

	foundAt := -1
	for i, link := range r.registeredNodes[f.Id] {
		if link.Name == f.Name &&
			((link.Parent == nil && f.Parent == nil) ||
				(link.Parent != nil && f.Parent != nil && link.Parent.Id == f.Parent.Id)) {
			foundAt = i
		}
	}
	if foundAt != -1 {
		r.registeredNodes[f.Id] = append(r.registeredNodes[f.Id][:foundAt], r.registeredNodes[f.Id][foundAt+1:]...)
	}

	if len(r.registeredNodes[f.Id]) == 0 {
		delete(r.registeredNodes, f.Id)
	}
}

func (r *registrar) nodeRegistered(f node) bool {
	r.RLock()
	defer r.RUnlock()
	for _, link := range r.registeredNodes[f.Id] {
		if link.Name == f.Name &&
			((link.Parent == nil && f.Parent == nil) ||
				(link.Parent != nil && f.Parent != nil && link.Parent.Id == f.Parent.Id)) {
			return true
		}
	}
	return false
}

func (r *registrar) getNode(id, parent uint64, name string) (node, bool) {
	r.RLock()
	defer r.RUnlock()
	for _, node := range r.registeredNodes[id] {
		if ((node.Parent == nil && parent == 0) || node.Parent.Id == parent) && node.Name == name {
			return node, true
		}
	}
	return node{}, false
}
