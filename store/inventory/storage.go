package inventory

import (
	"os"
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"go.uber.org/zap"
)

const (
	indexLocation = "/index"
)

func (d Driver) Sync() {
	d.persistInventory()
}

// persistInventory saves the file structure on disk starting with the root node
func (d Driver) persistInventory() {
	f, err := os.Create(d.location + indexLocation)
	if err != nil {
		log.Error("couldn't persist index at", zap.String("location", d.location), zap.Error(err))
	}
	defer f.Close()

	err = d.root.Serialize(f)
	if err != nil {
		log.Error("persisting storage index at", zap.String("location", d.location), zap.Error(err))
	}
}

func restoreInventory(location string) *store.File {
	f, err := os.OpenFile(location, os.O_RDONLY, store.ModeRegularFile)
	if err != nil {
		log.Error("couldn't open inventory index", zap.Error(err))
		now := time.Now()
		return &store.File{
			RWMutex:  &sync.RWMutex{},
			Id:       0,
			Mode:     store.ModeDirectory | 0777,
			Size:     1,
			Hash:     0,
			Children: nil,
			Atime:    now,
			Mtime:    now,
		}
	}
	defer f.Close()

	root := &store.File{}
	err = root.Deserialize(f)
	if err != nil {
		log.Fatal("couldn't read inventory index", zap.Error(err))
	}
	return root
}
