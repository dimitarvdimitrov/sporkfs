package data

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"go.uber.org/zap"
)

type index map[uint64]map[uint64]string // maps file ids to location

func (d *localDriver) Sync() {
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

// restoreIndex decodes the stored index at the location and returns it.
// If it doesn't exist, it returns an empty
func restoreIndex(location string) index {
	log.Debug("restoring file index", zap.String("from", location))
	index := make(index)
	f, err := os.Open(location + "/index")
	if err != nil {
		log.Error("couldn't load persisted index, starting fresh: %s", zap.Error(err))
		return map[uint64]map[uint64]string{}
	}
	defer f.Close()

	d := json.NewDecoder(f)
	err = d.Decode(&index)
	if err != nil {
		log.Error("couldn't load persisted index, starting fresh: %s", zap.Error(err))
		return map[uint64]map[uint64]string{}
	}
	return index
}

// generateStorageLocation returns a unique file name based on the file ID and the current hash
func generateStorageLocation(id, hash uint64) string {
	var msg [16]byte
	binary.BigEndian.PutUint64(msg[:8], id)
	binary.BigEndian.PutUint64(msg[8:], hash)

	hasher := sha1.New()
	_, _ = hasher.Write(msg[:])

	return (&big.Int{}).SetBytes(hasher.Sum(nil)).String()
}
