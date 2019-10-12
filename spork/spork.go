package spork

import (
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

var S Spork
var o = &sync.Once{}

type Spork struct {
}

func (s Spork) Root() store.File {
	return store.File{
		UUID: "00000001",
		Size: 0,
		Path: "/",
		Mode: 0555,
	}
}

func (s Spork) Readdir(path string) ([]store.File, error) {
	log.Debugf("readdir path: %s", path)
	var a []store.File
	o.Do(func() {
		a = []store.File{
			{
				UUID: "00000001",
				Path: "/my-file.21345rttxt",
				Size: 1,
				Mode: 0666,
			},
		}
	})
	defer func() { a = nil }()
	return a, nil
}

func (s Spork) Read(path string) ([]byte, error) {
	panic("implement me")
}

func (s Spork) Write(path string, offset uint64, data []byte) error {
	panic("implement me")
}

func (s Spork) Stat(path string) (store.File, error) {
	return store.File{
		UUID: time.Now().String(),
		Path: "some-name-right",
		Mode: 0555,
		Size: 1,
	}, nil
}
