package data

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
)

type index map[uint64]map[uint64]string // maps file ids to location on the local file system

// restoreIndex decodes the stored index at the location and returns it.
// If it doesn't exist, it returns an empty
func restoreIndex(location string) index {
	log.Debugf("restoring file index from %s", location)
	index := make(index)
	f, err := os.Open(location + "/index")
	if err != nil {
		log.Errorf("couldn't load persisted index, starting fresh: %s", err)
		return map[uint64]map[uint64]string{}
	}
	defer f.Close()

	d := json.NewDecoder(f)
	err = d.Decode(&index)
	if err != nil {
		log.Errorf("couldn't load persisted index, starting fresh: %s", err)
		return map[uint64]map[uint64]string{}
	}
	return index
}

// newStorageLocation returns a supposedly unique file name based on crypto/rand and the file ID
func newStorageLocation(id uint64) string {
	return fmt.Sprintf("%d-%d", id, rand.Int())
}
