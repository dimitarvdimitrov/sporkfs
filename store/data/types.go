package data

import (
	"io"
)

type Driver interface {
	// Contains will return true of false if the file is present. It will always return true for the zero-version file
	// regardless of the id.
	Contains(id, version uint64) bool
	ContainsAny(id uint64) bool

	// Open returns a reader and a writer, both using the same representation of the file
	// locally. If one of them is closed, the other one won't be usable. Both must be closed.
	// If the version is 0, an empty file will be created and returned.
	Open(id, oldVersion, newVersion uint64, flags int) (Reader, Writer, error)
	Reader(id, version uint64, flags int) (Reader, error)
	Remove(id, version uint64)
	Size(id, version uint64) int64

	// Write will return a Writer to the file and version with the flags.
	// If the version is 0, a new empty file will be created and returned.
	Writer(id, oldVersion, newVersion uint64, flags int) (Writer, error)
}

type Reader interface {
	io.ReaderAt
	io.ReadCloser
}

type Writer interface {
	Syncer
	io.WriterAt
	io.Writer
	Commit()
	Cancel()
}

type Syncer interface {
	Sync()
}
