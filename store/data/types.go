package data

import (
	"io"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

type Driver interface {
	Readerer
	Writerer
	ReadWriterer

	Add(id uint64, mode store.FileMode) (version uint64, err error)
	Contains(id, version uint64) bool
	PruneVersionsExcept(id, version uint64)
	Remove(id, version uint64)
	Size(id, version uint64) int64
	Sync()
}

// Open returns a reader and a writer, both using the same representation of the file
// locally. If one of them is closed, the other one won't be usable. Both must be closed.
type ReadWriterer interface {
	Open(id, version uint64, flags int) (Reader, Writer, error)
}

// TODO inplace these in the Driver interface
type Writerer interface {
	Writer(id, version uint64, flags int) (Writer, error)
}

type Readerer interface {
	Reader(id, version uint64, flags int) (Reader, error)
}

type Reader interface {
	io.ReaderAt
	io.ReadCloser
}

type Writer interface {
	HashCloser
	Flusher
	io.WriterAt
	io.Writer
}

type Flusher interface {
	Flush()
}

// HashCloser should return the new hash of a file on a close
type HashCloser interface {
	Close() uint64
}

type hashFunc func() uint64
