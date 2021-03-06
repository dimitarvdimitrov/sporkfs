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
			log.Debug("[spork] processing add raft entry", log.Id(req.Id), log.Name(req.Name))
			parent, err := s.inventory.GetAny(req.ParentId)
			if err != nil {
				log.Error("add raft entry unsuccessful", zap.Error(err))
				break
			}
			parent.Lock()

			// if it's a link we copy everything we know about the file
			file := s.newFile(req.Name, store.FileMode(req.Mode))
			file.Id = req.Id
			if existingFile, err := s.inventory.GetAny(file.Id); err == nil {
				file.RWMutex = existingFile.RWMutex
				file.Version = existingFile.Version
				file.Size = existingFile.Size
				file.Atime = existingFile.Atime
				file.Mtime = existingFile.Mtime
			}
			file.Lock()

			// we also need to make sure there isn't the same file locally in case this
			// is a race condition between changes from different nodes
			for i, c := range parent.Children {
				if c.Name == file.Name {
					parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
					parent.Size--
					s.delete(c)
					s.deleted <- c
					log.Debug("[spork] replacing file with same name")
					break
				}
			}

			s.add(file, parent)
			s.invalid <- parent

			parent.Unlock()
			file.Unlock()
		case *raftpb.Entry_Rename:
			req := msg.Rename
			log.Debug("[spork] processing rename raft entry", log.Id(req.Id), zap.String("new_name", req.NewName))

			file, err := s.inventory.GetSpecific(req.Id, req.OldParentId, req.OldName)
			if err != nil {
				log.Error("rename file for raft (file)", zap.Error(err))
				break
			}
			oldParent := file.Parent
			newParent, err := s.inventory.GetAny(req.NewParentId)
			if err != nil {
				log.Error("rename file for raft (new parent)", zap.Error(err))
				break
			}
			file.Lock()
			oldParent.Lock()
			if oldParent.Id != newParent.Id {
				newParent.Lock()
			}

			// we copy the file so that the rename below doesn't affect what the invalidator reads
			oldFile := *file
			s.invalid <- &oldFile

			s.rename(file, newParent, oldParent, req.NewName)

			file.Unlock()
			oldParent.Unlock()
			if oldParent.Id != newParent.Id {
				newParent.Unlock()
			}
		case *raftpb.Entry_Delete:
			req := msg.Delete
			log.Debug("[spork] processing delete raft entry", log.Id(req.Id))

			file, err := s.inventory.GetSpecific(req.Id, req.ParentId, req.Name)
			if err != nil {
				log.Error("[spork] delete file for raft", zap.Error(err))
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
			log.Debug("[spork] processing change raft entry", log.Id(req.Id), log.Ver(req.Version), zap.Uint64("from", req.PeerId))

			file, err := s.inventory.GetAny(req.Id)
			if err != nil {
				log.Error("get updated file for raft", zap.Error(err))
				break
			}

			file.Lock()
			oldVersion := file.Version
			now := time.Now()
			s.inventory.SetVersion(file.Id, req.Version)
			s.inventory.SetSize(file.Id, int64(req.Offset)+req.Size)
			file.Mtime, file.Atime = now, now

			peer := s.peers.GetPeerRaft(req.PeerId)

			if s.peers.IsLocalFile(req.Id) || s.cache.ContainsAny(req.Id) {
				var dest data.Driver = s.cache
				if s.peers.IsLocalFile(req.Id) {
					dest = s.data
				}

				if err := s.updateLocalFile(req.Id, oldVersion, req.Version, peer, dest); err != nil {
					log.Error("[spork] transferring changed file from raft", zap.Error(err))
				} else {
					if oldVersion != req.Version {
						dest.Remove(file.Id, oldVersion)
					}
				}
			}

			for _, link := range s.inventory.GetAll(file.Id) {
				s.invalid <- link
			}
			file.Unlock()
		}
		entry.Action()
		log.Debug("[spork] finished processing raft entry")
	}
}
