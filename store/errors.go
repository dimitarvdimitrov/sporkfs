package store

import "errors"

var (
	ErrNoSuchFile        = errors.New("[spork]: no such file or directory")
	ErrFileAlreadyExists = errors.New("[spork]: file exists")
	ErrDirectoryNotEmpty = errors.New("[spork]: directory not empty")
	ErrStaleHandle       = errors.New("[spork]: stale file handle")
)
