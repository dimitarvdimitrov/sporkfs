package data

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/minio/highwayhash"
)

var (
	hashKey, _ = hex.DecodeString("474c383279736a66674e48325037694c524e7a3830746e714636766f71675553")
)

type localDriver struct {
	storageRoot string
	indexM      sync.RWMutex
	index       index
}

func NewLocalDriver(location string) localDriver {
	return localDriver{
		storageRoot: location + "/",
		index:       restoreIndex(location),
	}
}

func (d localDriver) Add(id uint64, mode store.FileMode) (uint64, error) {
	d.indexM.Lock()
	defer d.indexM.Unlock()

	if _, ok := d.index[id]; ok {
		return 0, store.ErrFileAlreadyExists
	}

	if mode&store.ModeDirectory != 0 {
		return 0, nil // noop if it's a dir
	}

	filePath := newFilePath(id)

	f, err := os.OpenFile(d.storageRoot+filePath, os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	log.Debugf("created internal file %s for file id %d", filePath)

	hash := hashHandle(f)
	d.index[id] = map[uint64]string{hash: filePath}

	return hash, nil
}

func hashPath(path string) uint64 {
	file, err := os.Open(path)
	if err != nil {
		// retry
		file, err = os.Open(path)
		if err != nil {
			log.Errorf("couldn't open file to hash: %s", err)
			return 0
		}
	}
	defer file.Close()
	return hashHandle(file)
}

func hashHandle(file *os.File) uint64 {
	hash, err := highwayhash.New64(hashKey)
	if err != nil {
		// retry
		hash, err = highwayhash.New64(hashKey)
		if err != nil {
			log.Errorf("couldn't start hashing file: %s", err)
			return 0
		}
	}

	_, err = io.Copy(hash, file)
	if err != nil {
		// retry
		_, err = io.Copy(hash, file)
		if err != nil {
			log.Errorf("couldn't hash file: %s", err)
		}
	}
	return hash.Sum64()
}

func (d localDriver) Contains(id, hash uint64) bool {
	d.indexM.RLock()
	defer d.indexM.RUnlock()

	_, exists := d.index[id][hash]
	return exists
}

// TODO remove
func (d localDriver) PruneVersionsExcept(id, hash uint64) {
	hashToPrune := make([]uint64, 0, len(d.index[id]))
	for v := range d.index[id] {
		if v == hash {
			continue
		}
		hashToPrune = append(hashToPrune, v)
	}

	for _, v := range hashToPrune {
		d.Remove(id, v)
	}
}

func (d localDriver) Remove(id, hash uint64) {
	if !d.Contains(id, hash) {
		return
	}

	d.indexM.Lock()
	path := d.index[id][hash]
	delete(d.index[id], hash)
	d.indexM.Unlock()

	removeFromDisk(d.storageRoot + path)
}

func removeFromDisk(path string) {
	err := os.Remove(path)
	if err != nil {
		log.Errorf("couldn't remove file: %s", err)
	}
}

func (d localDriver) Reader(id, hash uint64, flags int) (Reader, error) {
	d.indexM.RLock()
	location, exists := d.index[id][hash]
	d.indexM.RUnlock()
	if !exists {
		return nil, store.ErrNoSuchFile
	}

	f, err := os.OpenFile(d.storageRoot+location, flags, store.ModeRegularFile)
	if err != nil {
		return nil, fmt.Errorf("file id=%d was in index but not on disk: %w", id, err)
	}

	segReader := &segmentedReader{
		f: f,
		onClose: func() {
			_ = f.Close()
		},
	}

	return segReader, nil
}

func (d localDriver) Writer(id, hash uint64, flags int) (Writer, error) {
	d.indexM.RLock()
	fileLocation, exists := d.index[id][hash]
	d.indexM.RUnlock()
	if !exists {
		return nil, store.ErrNoSuchFile
	}

	newFilename := newFilePath(id)
	newFilePath := d.storageRoot + newFilename
	err := duplicateFile(d.storageRoot+fileLocation, newFilePath)
	if err != nil {
		return nil, err
	}

	if flags&(os.O_TRUNC|os.O_APPEND) == 0 {
		flags |= os.O_TRUNC
	}

	if flags&os.O_CREATE != 0 { // we've already duplicated the file, it's already created
		flags ^= os.O_CREATE
	}

	file, err := os.OpenFile(newFilePath, flags, store.ModeRegularFile)
	if err != nil {
		return nil, err
	}

	var newHash uint64

	onClose := func() {
		_ = file.Sync()
		_ = file.Close()

		newHash = hashPath(newFilePath)
		if newHash == hash {
			removeFromDisk(newFilePath)
		} else {
			d.indexM.Lock()
			defer d.indexM.Unlock()
			d.index[id][newHash] = newFilename
		}
	}

	getHash := func() uint64 {
		return newHash
	}

	segWriter := &segmentedWriter{
		f:       file,
		flush:   flusher(file),
		onClose: onClose,
		hash:    getHash,
	}

	return segWriter, nil
}

func flusher(f *os.File) func() {
	return func() {
		_ = f.Sync()
	}
}

func duplicateFile(oldPath, newPath string) error {
	source, err := os.Open(oldPath)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(newPath)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}

func (d localDriver) Size(id, hash uint64) int64 {
	d.indexM.RLock()
	descriptor, err := os.Open(d.storageRoot + d.index[id][hash])
	if err != nil {
		return 0
	}
	d.indexM.RUnlock()
	defer descriptor.Close()

	info, err := descriptor.Stat()
	if err != nil {
		return 0
	}

	return info.Size()
}

func (d localDriver) Sync() {
	d.persistIndex()
}

func (d localDriver) persistIndex() {
	f, err := os.Create(d.storageRoot + "/index")
	if err != nil {
		log.Errorf("couldn't persist index at %s: %s", d.storageRoot, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(d.index)
	if err != nil {
		log.Errorf("persisting storage index at %s: %w", d.storageRoot, err)
	}
}
