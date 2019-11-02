package store

import "errors"

var (
	ErrNoSuchFile        = errors.New("no such file or directory")
	ErrFileAlreadyExists = errors.New("file exists")
	ErrDirectoryNotEmpty = errors.New("directory not empty")
)
