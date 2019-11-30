package data

import (
	"encoding/json"
	"fmt"
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
		log.Errorf("couldn't load persisted index, starting fresh: %w", err)
		return map[uint64]map[uint64]string{}
	}
	defer f.Close()

	d := json.NewDecoder(f)
	err = d.Decode(&index)
	if err != nil {
		log.Errorf("couldn't load persisted index, starting fresh: %w", err)
		return map[uint64]map[uint64]string{}
	}
	return index
}

// newFilePath returns a supposedly unique file name based on crypto/rand and the file ID
func newFilePath(id uint64) string {
	//randomBytes := make([]byte, 8)
	//var err error
	//
	//for retries := 0; (retries == 0) || (err != nil && retries < 10); retries++ {
	//	_, err = rand.Read(randomBytes)
	//}
	//if err != nil {
	//	panic("couldn't generate a random number")
	//}

	return fmt.Sprintf("%x", id)
}
