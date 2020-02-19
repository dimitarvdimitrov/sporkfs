package spork

import (
	"time"

	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"go.uber.org/zap"
)

func (s Spork) watchRaft() {
	defer s.wg.Done()
	for entry := range s.commitC {
		switch msg := entry.Message.(type) {
		case *raftpb.Entry_Add:
			req := msg.Add
			log.Debug("processing add raft entry", log.Id(req.Id), log.Name(req.Name))
			parent, err := s.inventory.Get(req.ParentId)
			if err != nil {
				log.Error("add raft entry unsuccessful", zap.Error(err))
				break
			}
			parent.Lock()

			file := s.newFile(req.Name, store.FileMode(req.Mode))
			file.Id = req.Id
			file.Lock()

			s.add(file, parent)
			s.invalid <- parent

			parent.Unlock()
			file.Unlock()
		case *raftpb.Entry_Rename:
			req := msg.Rename
			log.Debug("processing rename raft entry", log.Id(req.Id), zap.String("new_name", req.NewName))

			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Error("rename file for raft (file)", zap.Error(err))
				break
			}
			oldParent := file.Parent
			newParent, err := s.inventory.Get(req.NewParentId)
			if err != nil {
				log.Error("rename file for raft (new parent)", zap.Error(err))
				break
			}
			file.Lock()
			oldParent.Lock()
			if oldParent.Id != newParent.Id {
				newParent.Lock()
			}

			s.rename(file, newParent, oldParent, req.NewName)
			s.invalid <- file

			file.Unlock()
			oldParent.Unlock()
			if oldParent.Id != newParent.Id {
				newParent.Unlock()
			}
		case *raftpb.Entry_Delete:
			req := msg.Delete
			log.Debug("processing delete raft entry", log.Id(req.Id))

			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Error("delete file for raft (file)", zap.Error(err))
				break
			}
			file.Lock()
			file.Parent.Lock()
			s.delete(file)
			s.deleted <- file
			file.Unlock()
			file.Parent.Unlock()
		case *raftpb.Entry_Change:
			req := msg.Change
			log.Debug("processing change raft entry", log.Id(req.Id), log.Hash(req.Hash))

			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Error("get updated file for raft", zap.Error(err))
				break
			}

			file.Lock()
			oldHash := file.Hash
			now := time.Now()
			file.Hash = req.Hash
			file.Size = int64(req.Offset) + req.Size
			file.Mtime, file.Atime = now, now

			peer := s.peers.GetPeerRaft(req.PeerId)

			if s.peers.IsLocalFile(req.Id) || s.cache.ContainsAny(req.Id) {
				var dest data.Driver = s.cache
				if s.peers.IsLocalFile(req.Id) {
					dest = s.data
				}

				if err := s.updateLocalFile(req.Id, oldHash, req.Hash, peer, dest); err != nil {
					log.Error("transferring changed file from raft", zap.Error(err))
				} else {
					if oldHash != req.Hash {
						dest.Remove(file.Id, oldHash)
					}
				}
			}

			s.invalid <- file
			file.Unlock()
		}
		entry.Action()
		log.Debug("finished processing raft entry")
	}
}
