package state

import (
	"path"
	"strings"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

const (
	dirSep = "/"
)

type fileNode struct {
	*store.File
	children []*fileNode
}

func rootname(path string) string {
	if sepIndex := strings.Index(path, dirSep); sepIndex > 0 {
		return path[:sepIndex]
	} else {
		return path
	}
}

func stripRootSegment(p string) string {
	root := path.Base(p)
	if root == p {
		return ""
	}
	return p[strings.Index(p, root)+len(root)+1:]
}

func convertNodes(files []*fileNode) []*store.File {
	result := make([]*store.File, len(files))
	for i, file := range files {
		result[i] = file.File
	}
	return result
}
