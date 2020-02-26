package raft

import (
	"context"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

// Committer tries to commit the entry to raft. It returns true or false if the entry get completed in a timely
// manner. After proposing a change, which has been approved by raft, you need to invoke the callback
// function returned by the Committer's method. Even if you fail to action the result, you need to invoke the callback;
// otherwise bad things will happen. If the change wasn't approved you don't need to call the callback,
// and calling it will be a noop.
type Committer interface {
	Add(id, parentId uint64, name string, mode store.FileMode) (bool, func())
	Change(id, version, offset uint64, size int64) (bool, func())
	Rename(id, oldParentId, newParentId uint64, newName string) (bool, func())
	Delete(id, parentId uint64) (bool, func())
}

type Raft struct {
	n *node
	a *applier
}

func New(cfg Config, states ...StateSource) (*Raft, <-chan UnactionedMessage, *Peers) {
	peers := NewPeerList(cfg)
	n, commits, proposals := newNode(peers, cfg.DataDir, states...)
	a, syncC := newApplier(commits, proposals)

	return &Raft{
		a: a,
		n: n,
	}, syncC, peers
}

func (r *Raft) Add(id, parentId uint64, name string, mode store.FileMode) (bool, func()) {
	return r.a.ProposeAdd(id, parentId, name, mode)
}

func (r *Raft) Change(id, version, offset uint64, size int64) (bool, func()) {
	return r.a.ProposeChange(id, version, offset, r.n.peers.thisPeerRaftId(), size)
}

func (r *Raft) Rename(id, oldParentId, newParentId uint64, newName string) (bool, func()) {
	return r.a.ProposeRename(id, oldParentId, newParentId, newName)
}

func (r *Raft) Delete(id, parentId uint64) (bool, func()) {
	return r.a.ProposeDelete(id, parentId)
}

func (r *Raft) Step(ctx context.Context, e *etcdraftpb.Message) (*raftpb.Empty, error) {
	return &raftpb.Empty{}, r.n.raft.Step(ctx, *e)
}

func (r *Raft) Shutdown() {
	log.Info("stopping raft...")
	r.n.close()
	r.a.close()
	log.Info("stopped raft")
}
