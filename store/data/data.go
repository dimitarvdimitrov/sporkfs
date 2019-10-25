package data

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

type localDriver struct {
	location string
	index    map[uint64]string // maps file ids to location on the local file system
}

func NewLocalDriver(location string) localDriver {
	return localDriver{
		location: location,
		index:    restoreIndex(location),
	}
}

func restoreIndex(location string) map[uint64]string {
	log.Debugf("restoring file index from %s", location)
	index := map[uint64]string{}
	f, err := os.Open(location + "/index")
	if err != nil {
		log.Fatal("couldn't load persisted index, starting fresh: %w", err)
		return nil
	}
	defer f.Close()

	d := json.NewDecoder(f)
	err = d.Decode(&index)
	if err != nil {
		log.Errorf("couldn't load persisted index, starting fresh: %w", err)
		return map[uint64]string{}
	}
	return index
}

func (d localDriver) Read(file *store.File, offset, size uint64) ([]byte, error) {
	location, exists := d.index[file.Id]
	if !exists {
		return nil, fmt.Errorf("local disk: %w", store.ErrNoSuchFile)
	}
	f, err := os.Open(d.location + "/" + location)
	if err != nil {
		panic(fmt.Errorf("file id=%d was in index but not on disk: %w", file.Id, err))
	}
	defer f.Close()

	data := make([]byte, min(size, file.Size-offset))
	n, err := f.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data[:n], nil
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func (d localDriver) Sync() {
	d.persistIndex()
}

func (d localDriver) persistIndex() {
	f, err := os.Create(d.location + "/index")
	if err != nil {
		log.Errorf("couldn't persist index at %s: %w", d.location, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(d.index)
	if err != nil {
		log.Errorf("persisting storage index at %s: %w", d.location, err)
	}
}
