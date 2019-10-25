package data

import "github.com/dimitarvdimitrov/sporkfs/store"

// TODO maybe get rid of the interface and have only the struct?
type Driver interface {
	Read(file *store.File, offset, size uint64) ([]byte, error)
	Write(file *store.File, offset int64, data []byte, flags int) (int, error)
	Sync()
	Size(f *store.File) int
}
