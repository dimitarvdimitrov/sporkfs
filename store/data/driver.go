package data

import (
	"io"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

// TODO maybe get rid of the interface and have only the struct?
type Driver interface {
	Readerer
	Writerer

	Add(id uint64, mode store.FileMode) (version uint64, err error)
	Contains(id, version uint64) bool
	PruneVersionsExcept(id, version uint64)
	Remove(id, version uint64)
	Read(id, version uint64, offset, size int64) ([]byte, error)
	Size(id, version uint64) int64
	Sync()
}

// TODO add godocs here
type Writerer interface {
	Writer(id, version uint64, offset int64, flags int) (w io.WriteCloser, getHash func() uint64, err error)
}

// TODO add godocs here
type Readerer interface {
	Reader(id, version uint64, offset, size int64) (io.ReadCloser, error)
}
