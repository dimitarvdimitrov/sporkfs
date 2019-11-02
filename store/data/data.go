package data

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

type localDriver struct {
	location string
	index    map[uint64]string // maps file ids to location on the local file system
}

func NewLocalDriver(location string) localDriver {
	return localDriver{
		location: location + "/",
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

func (d localDriver) Add(file *store.File) error {
	if _, ok := d.index[file.Id]; ok {
		return store.ErrFileAlreadyExists
	}

	if file.Mode&store.ModeDirectory != 0 {
		return nil // noop if it's a dir
	}

	filePath := strconv.FormatUint(file.Id, 16)
	d.index[file.Id] = filePath
	f, err := os.OpenFile(d.location+filePath, os.O_CREATE|os.O_EXCL, file.Mode)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}

func (d localDriver) Remove(id uint64) {
	log.Error(os.Remove(d.location + d.index[id]))
	delete(d.index, id)
}

func (d localDriver) Read(file *store.File, offset, size uint64) ([]byte, error) {
	location, exists := d.index[file.Id]
	if !exists {
		return nil, fmt.Errorf("local disk: %w", store.ErrNoSuchFile)
	}
	f, err := os.Open(d.location + location)
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

func (d localDriver) Write(f *store.File, offset int64, data []byte, flags int) (int, error) {
	location, exists := d.index[f.Id]
	if !exists {
		return 0, store.ErrNoSuchFile
	}

	if flags&os.O_APPEND != 0 {
		return write(d.location+location, data, flags)
	} else if offset > 0 {
		return writeAt(d.location+location, offset, data, flags)
	} else {
		if flags&os.O_TRUNC == 0 && flags&os.O_CREATE == 0 {
			flags = flags | os.O_TRUNC
		}
		return write(d.location+location, data, flags)
	}
}

func writeAt(path string, offset int64, data []byte, flags int) (int, error) {
	f, err := os.OpenFile(path, flags, store.ModeRegularFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.WriteAt(data, offset)
}

func write(path string, data []byte, flags int) (int, error) {
	f, err := os.OpenFile(path, flags, store.ModeRegularFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.Write(data)
}

func (d localDriver) Size(f *store.File) int {
	descriptor, err := os.Open(d.location + d.index[f.Id])
	if err != nil {
		return 0
	}
	defer descriptor.Close()

	info, err := descriptor.Stat()
	if err != nil {
		return 0
	}

	return int(info.Size())
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
