package store

import "errors"

var (
	ErrNoSuchFile        = errors.New("no such file")
	ErrFileAlreadyExists = errors.New("file already exists")
)
