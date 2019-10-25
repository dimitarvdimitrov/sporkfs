package store

import (
	"os"
)

type FileMode = os.FileMode

const (
	ModeDirectory   FileMode = os.ModeDir
	ModeRegularFile FileMode = 0666
)

type File struct {
	Id   uint64
	Name string
	Mode FileMode
	Size uint64
}
