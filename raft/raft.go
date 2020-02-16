package raft

import (
	"context"

	"github.com/dimitarvdimitrov/sporkfs/log"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Committer interface {
	Add(id, parentId uint64, name string, mode store.FileMode) bool
	Change(id, hash, offset uint64, size int64) bool
	Rename(id, oldParentId, newParentId uint64, newName string) bool
	Delete(id, parentId uint64) bool
}

type Raft struct {
	n *node
	a *applier
}

func New(peers *Peers) (*Raft, <-chan *raftpb.Entry) {
	n, commits, proposals := newNode(peers)
	a, syncC := newApplier(commits, proposals)

	return &Raft{
		a: a,
		n: n,
	}, syncC
}

func (r *Raft) Add(id, parentId uint64, name string, mode store.FileMode) bool {
	return r.a.ProposeAdd(id, parentId, name, mode)
}

func (r *Raft) Change(id, hash, offset uint64, size int64) bool {
	return r.a.ProposeChange(id, hash, offset, r.n.peers.thisPeerRaftId(), size)
}

func (r *Raft) Rename(id, oldParentId, newParentId uint64, newName string) bool {
	return r.a.ProposeRename(id, oldParentId, newParentId, newName)
}

func (r *Raft) Delete(id, parentId uint64) bool {
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
