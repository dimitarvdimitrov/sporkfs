package fuse

import "sync"

type nodeRegistrar interface {
	registerNode(node)
	deleteNode(uint64)
	nodeRegistered(node) bool
	getNode(uint64) (node, bool)
}

type registrar struct {
	*sync.RWMutex

	registeredNodes map[uint64]node
}

func (r *registrar) registerNode(n node) {
	r.Lock()
	defer r.Unlock()
	r.registeredNodes[n.Id] = n
}

func (r *registrar) deleteNode(id uint64) {
	r.Lock()
	defer r.Unlock()
	delete(r.registeredNodes, id)
}

func (r *registrar) nodeRegistered(n node) (ok bool) {
	r.RLock()
	defer r.RUnlock()
	_, ok = r.registeredNodes[n.Id]
	return
}

func (r *registrar) getNode(id uint64) (n node, ok bool) {
	r.RLock()
	defer r.RUnlock()
	n, ok = r.registeredNodes[id]
	return
}
