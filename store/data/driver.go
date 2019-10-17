package data

import "github.com/dimitarvdimitrov/sporkfs/store"

type Driver interface {
	Read(file *store.File, offset, size uint64) ([]byte, error)
	Sync()
	//Write(file *store.File, offset, size uint64, data []byte) (uint64, error)
}
