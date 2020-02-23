package data

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"go.uber.org/zap"
)

type index map[uint64]map[uint64]string // maps file ids to location
// TODO remove
func (d *localDriver) Sync() {
	log.Info("persisting data index")
	d.persistIndex()
}

func (d *localDriver) persistIndex() {
	f, err := os.Create(d.storageRoot + "/index")
	if err != nil {
		log.Error("couldn't persist index", zap.String("location", d.storageRoot), zap.Error(err))
		return
	}
	defer f.Close()

	d.indexM.RLock()
	err = json.NewEncoder(f).Encode(d.index)
	d.indexM.RUnlock()
	if err != nil {
		log.Error("persisting storage index", zap.String("location", d.storageRoot), zap.Error(err))
	}
}

// buildIndex decodes the stored index at the location and returns it.
// If it doesn't exist, it returns an empty
func buildIndex(location string) (idx index) {
	log.Debug("restoring file index", zap.String("from", location))
	idx = make(index)

	storageDir, err := os.Open(location)
	if err != nil {
		log.Error("[data] couldn't read local files dir; starting fresh")
		return
	}
	files, err := storageDir.Readdir(-1)
	if err != nil {
		log.Error("[data] couldn't read existing local files; starting fresh")
		return
	}

	for _, f := range files {
		nameComponents := strings.SplitN(f.Name(), "-", -1)
		if len(nameComponents) != 2 {
			continue
		}
		id, err1 := strconv.ParseUint(nameComponents[0], 10, 64)
		version, err2 := strconv.ParseUint(nameComponents[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		if idx[id] == nil {
			idx[id] = make(map[uint64]string)
		}
		idx[id][version] = f.Name()
	}
	return
}

func generateStorageLocation(id, version uint64) string {
	return fmt.Sprintf("%d-%d", id, version)
}
