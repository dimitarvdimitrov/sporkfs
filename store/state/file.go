package state

import (
	"github.com/dimitarvdimitrov/sporkfs/store"
)

const (
	dirSep = "/"
)

type fileNode = store.File

func convertNodes(files []*store.File) []*store.File {
	result := make([]*store.File, len(files))
	for i, file := range files {
		result[i] = file
	}
	return result
}
