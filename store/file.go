package store

import (
	"os"
	"sync"
)

type FileMode = os.FileMode

const (
	ModeDirectory   FileMode = os.ModeDir
	ModeRegularFile FileMode = 0666
)

type File struct {
	*sync.RWMutex

	Id   uint64
	Name string
	Mode FileMode
	Size uint64 // TODO change to int64

	Children []*File
}

// Copy copies all the fields of this file to a new struct, except for the RWLock; the RWLock is kept the same
func (f *File) Copy() *File {
	localCopy := *f
	return &localCopy
}
