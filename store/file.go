package store

import (
	"os"
)

type FileMode = os.FileMode

const (
	// TODO rename to contain mode
	Directory   FileMode = os.ModeDir
	RegularFile FileMode = 0666
)

type File struct {
	Id   uint64
	Name string
	Mode FileMode
	Size uint64
}
