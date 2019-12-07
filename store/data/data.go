package data

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/minio/highwayhash"
)

var (
	hashKey, _ = hex.DecodeString("474c383279736a66674e48325037694c524e7a3830746e714636766f71675553")
)

type localDriver struct {
	storageRoot string
	index       index
}

func NewLocalDriver(location string) localDriver {
	return localDriver{
		storageRoot: location + "/",
		index:       restoreIndex(location),
	}
}

func (d localDriver) Add(id uint64, mode store.FileMode) (uint64, error) {
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
	_, exists := d.index[id][hash]
	return exists
}

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
	path, ok := d.index[id][hash]
	if !ok {
		return
	}
	removeFromDisk(d.storageRoot + path)
	delete(d.index[id], hash)
}

func removeFromDisk(path string) {
	err := os.Remove(path)
	if err != nil {
		log.Errorf("couldn't remove file: %s", err)
	}
}

func (d localDriver) Reader(id, hash uint64, offset, size int64) (io.ReadCloser, error) {
	location, exists := d.index[id][hash]
	if !exists {
		return nil, fmt.Errorf("local disk: %w", store.ErrNoSuchFile)
	}
	f, err := os.Open(d.storageRoot + location)
	if err != nil {
		return nil, fmt.Errorf("file id=%d was in index but not on disk: %w", id, err)
	}

	segReader := &segmentedReader{
		offset: offset,
		size:   size,
		f:      f,
		onClose: func() {
			_ = f.Close()
		},
	}

	return segReader, nil
}

func (d localDriver) Writer(id, hash uint64, flags int) (Writer, error) {
	fileLocation, exists := d.index[id][hash]
	if !exists {
		return nil, store.ErrNoSuchFile
	}

	newFilename := newFilePath(id)
	newFilePath := d.storageRoot + newFilename
	err := duplicateFile(d.storageRoot+fileLocation, newFilePath)
	if err != nil {
		return nil, err
	}

	if flags&os.O_TRUNC == 0 && flags&os.O_CREATE == 0 && flags&os.O_APPEND == 0 {
		flags |= os.O_TRUNC
	}

	if flags&os.O_APPEND != 0 { // we will use the file for all purposes
		flags ^= os.O_APPEND
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

		computedHash := hashPath(newFilePath)
		if computedHash == hash {
			removeFromDisk(newFilePath)
		} else {
			d.index[id][computedHash] = newFilename
		}
		newHash = computedHash
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
	descriptor, err := os.Open(d.storageRoot + d.index[id][hash])
	if err != nil {
		return 0
	}
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
