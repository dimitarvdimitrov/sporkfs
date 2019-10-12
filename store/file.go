package store

import "os"

type File struct {
	UUID string
	Path string
	Mode os.FileMode
	Size uint64
}
