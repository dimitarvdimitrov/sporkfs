package spork

import (
	"time"

	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

func (s Spork) watchRaft() {
	for entry := range s.commitC {
		switch msg := entry.Message.(type) {
		case *raftpb.Entry_Add:
			req := msg.Add
			parent, err := s.inventory.Get(req.ParentId)
			if err != nil {
				log.Errorf("actioning raft committed entry: %s", err)
				break
			}
			parent.Lock()

			file := s.newFile(req.Name, store.FileMode(req.Mode))
			file.Id = req.Id
			file.Lock()

			err = s.createLocally(file, parent)
			if err != nil {
				log.Errorf("can't add committed raft file locally: %s", err)
			}
			parent.Unlock()
			file.Unlock()
		case *raftpb.Entry_Rename:
			req := msg.Rename
			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Errorf("rename file for raft (file): %s", err)
			}
			oldParent, err := s.inventory.Get(req.OldParentId)
			if err != nil {
				log.Errorf("rename file for raft (old parent): %s", err)
			}
			newParent, err := s.inventory.Get(req.NewParentId)
			if err != nil {
				log.Errorf("rename file for raft (new parent): %s", err)
			}

			s.renameLocally(file, newParent, oldParent, req.NewName)
		case *raftpb.Entry_Delete:
			req := msg.Delete
			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Errorf("rename file for raft (file): %s", err)
			}
			parent, err := s.inventory.Get(req.ParentId)
			if err != nil {
				log.Errorf("rename file for raft (old parent): %s", err)
			}
			file.Lock()
			parent.Lock()

			index := -1
			for i, c := range parent.Children {
				if c.Id == file.Id {
					index = i
					break
				}
			}

			if index == -1 {
				log.Errorf("couldn't find child in parent file to remove (raft)")
			} else {
				s.deleteLocally(file, parent, index)
			}

			file.Unlock()
			parent.Unlock()
		case *raftpb.Entry_Change:
			req := msg.Change

			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Errorf("get updated file for raft: %s", err)
				break
			}
			file.Lock()
			if s.peers.IsLocalFile(req.Id) {
				err := s.transferRemoteFile(req.Id, req.Hash, s.data)
				if err != nil {
					log.Error("transferring changed local file from raft: %s", err)
				}
			} else {
				if s.cache.ContainsAny(req.Hash) {
					err := s.transferRemoteFile(req.Id, req.Hash, s.cache)
					if err != nil {
						log.Error("transferring changed local file from raft: %s", err)
					}
				}
			}

			file.Hash = req.Hash
			file.Size = s.data.Size(file.Id, file.Hash)
			file.Mtime = time.Now()
			file.Unlock()
			s.invalid <- file
		}
	}
}
