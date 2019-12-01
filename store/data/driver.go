package data

import "github.com/dimitarvdimitrov/sporkfs/store"

// TODO maybe get rid of the interface and have only the struct?
type Driver interface {
	Add(id uint64, mode store.FileMode) (version uint64, err error)
	PruneVersionsExcept(id, version uint64)
	Read(id, version uint64, offset, size int64) ([]byte, error)
	Remove(id, version uint64)
	Size(id, version uint64) int64
	Sync()
	Write(id, version uint64, offset int64, data []byte, flags int) (bytesWritten int, newVersion uint64, err error)
}
