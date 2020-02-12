package raft

import (
	"context"

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
	w *applier
}

func New(peers *Peers) (*Raft, <-chan *raftpb.Entry) {
	n, commits, proposals := newNode(peers)
	w, syncC := newWait(commits, proposals)

	return &Raft{
		w: w,
		n: n,
	}, syncC
}

func (r *Raft) Add(id, parentId uint64, name string, mode store.FileMode) bool {
	return r.w.ProposeAdd(id, parentId, name, mode)
}

func (r *Raft) Change(id, hash, offset uint64, size int64) bool {
	return r.w.ProposeChange(id, hash, offset, size)
}

func (r *Raft) Rename(id, oldParentId, newParentId uint64, newName string) bool {
	return r.w.ProposeRename(id, oldParentId, newParentId, newName)
}

func (r *Raft) Delete(id, parentId uint64) bool {
	return r.w.ProposeDelete(id, parentId)
}

func (r *Raft) Step(ctx context.Context, e *etcdraftpb.Message) (*raftpb.Empty, error) {
	return &raftpb.Empty{}, r.n.raft.Step(ctx, *e)
}

func (r *Raft) Shutdown() {
	r.n.close()
	r.w.wg.Wait()
}
