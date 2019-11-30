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
		log.Errorf("couldn't open file to hash: %s", err)
		return 0
	}
	defer file.Close()
	return hashHandle(file)
}

func hashHandle(file *os.File) uint64 {
	hash, err := highwayhash.New64(hashKey)
	if err != nil {
		log.Errorf("couldn't start hashing file: %s", err)
		return 0
	}

	_, err = io.Copy(hash, file)
	if err != nil {
		log.Errorf("couldn't hash file: %s", err)
	}
	return hash.Sum64()
}

func (d localDriver) PruneVersionsExcept(id, version uint64) {
	versionToPrune := make([]uint64, 0, len(d.index[id]))
	for v := range d.index[id] {
		if v == version {
			continue
		}
		versionToPrune = append(versionToPrune, v)
	}

	for _, v := range versionToPrune {
		d.Remove(id, v)
	}
}

func (d localDriver) Remove(id, version uint64) {
	path, ok := d.index[id][version]
	if !ok {
		return
	}
	removeFromDisk(d.storageRoot + path)
	delete(d.index[id], version)
}

func removeFromDisk(path string) {
	err := os.Remove(path)
	if err != nil {
		log.Errorf("couldn't remove file: %s", err)
	}
}

func (d localDriver) Read(id, version uint64, offset, size int64) ([]byte, error) {
	location, exists := d.index[id][version]
	if !exists {
		return nil, fmt.Errorf("local disk: %w", store.ErrNoSuchFile)
	}
	f, err := os.Open(d.storageRoot + location)
	if err != nil {
		panic(fmt.Errorf("file id=%d was in index but not on disk: %w", id, err))
	}
	defer f.Close()

	data := make([]byte, size)
	n, err := f.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data[:n], nil
}

func min(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func (d localDriver) Write(id, hash uint64, offset int64, data []byte, flags int) (int, uint64, error) {
	fileLocation, exists := d.index[id][hash]
	if !exists {
		return 0, 0, store.ErrNoSuchFile
	}

	var written int
	var err error

	newFilename := fileLocation + "1"
	err = duplicateFile(d.storageRoot+fileLocation, d.storageRoot+newFilename)
	if err != nil {
		return 0, 0, err
	}

	if flags&os.O_APPEND != 0 {
		written, err = write(d.storageRoot+newFilename, data, flags)
	} else if offset > 0 {
		written, err = writeAt(d.storageRoot+newFilename, offset, data, flags)
	} else {
		if flags&os.O_TRUNC == 0 && flags&os.O_CREATE == 0 {
			flags = flags | os.O_TRUNC
		}
		written, err = write(d.storageRoot+newFilename, data, flags)
	}

	if err != nil {
		return written, 0, err
	}

	newHash := hashPath(d.storageRoot + newFilename)
	if newHash == hash {
		removeFromDisk(d.storageRoot + newFilename)
	} else {
		d.index[id][newHash] = newFilename
	}
	return written, newHash, nil
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

func (d localDriver) Size(fId, version uint64) int64 {
	descriptor, err := os.Open(d.storageRoot + d.index[fId][version])
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
