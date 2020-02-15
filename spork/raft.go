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
			log.Debugf("processing add raft entry id:%d, name:%s", req.Id, req.Name)
			parent, err := s.inventory.Get(req.ParentId)
			if err != nil {
				log.Errorf("actioning raft committed entry: %s", err)
				break
			}
			parent.Lock()

			file := s.newFile(req.Name, store.FileMode(req.Mode))
			file.Id = req.Id
			file.Lock()

			if err = s.inventory.Add(file); err != nil {
				file.Unlock()
				parent.Unlock()
				log.Errorf("adding new raft file to inventory: %s", err)
				break
			}

			parent.Children = append(parent.Children, file)
			parent.Size = int64(len(parent.Children))
			file.Parent = parent

			if s.peers.IsLocalFile(file.Id) {
				if err = s.createInCacheOrData(file, parent); err != nil {
					log.Errorf("can't add committed raft file locally: %s", err)
				}
			}
			s.invalid <- parent
			parent.Unlock()
			file.Unlock()
		case *raftpb.Entry_Rename:
			req := msg.Rename
			log.Debugf("processing rename raft entry id:%d, new_name:%s", req.Id, req.NewName)

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
			s.invalid <- file
		case *raftpb.Entry_Delete:
			req := msg.Delete
			log.Debugf("processing delete raft entry id:%d", req.Id)

			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Errorf("delete file for raft (file): %s", err)
			}
			parent, err := s.inventory.Get(req.ParentId)
			if err != nil {
				log.Errorf("delete file for raft (old parent): %s", err)
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
				log.Error("couldn't find child in parent file to remove (raft)")
			} else {
				s.deleteLocally(file, parent, index)
			}
			s.deleted <- file
			file.Unlock()
			parent.Unlock()
		case *raftpb.Entry_Change:
			req := msg.Change
			log.Debugf("processing change raft entry id:%d, hash:%d", req.Id, req.Hash)

			file, err := s.inventory.Get(req.Id)
			if err != nil {
				log.Errorf("get updated file for raft: %s", err)
				break
			}
			file.Lock()
			if peer := s.peers.GetPeerRaft(req.PeerId); s.peers.IsLocalFile(req.Id) {
				err := s.updateLocalFile(req.Id, file.Hash, req.Hash, peer, s.data)
				if err != nil {
					log.Errorf("transferring changed local file from raft: %s", err)
				}
			} else {
				if s.cache.ContainsAny(req.Id) {
					err := s.updateLocalFile(req.Id, file.Hash, req.Hash, peer, s.cache)
					if err != nil {
						log.Errorf("transferring changed cached file from raft: %s", err)
					}
				}
			}

			file.Hash = req.Hash
			file.Size = s.data.Size(file.Id, file.Hash)
			file.Mtime = time.Now()
			s.invalid <- file
			file.Unlock()
		}
		log.Debug("finished processing raft entry")
	}
}
