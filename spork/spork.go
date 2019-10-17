package spork

import (
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store/state"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

var S = Spork{
	state: state.NewDriver(""),
}

type Spork struct {
	state state.Driver
}

func (s Spork) Root() *store.File {
	return s.state.Root()
}

func (s Spork) Readdir(path string) ([]*store.File, error) {
	return s.state.ReadDir(path)
}

func (s Spork) ChildrenOf(id uint64) ([]*store.File, error) {
	return s.state.ChildrenOf(id), nil
}

func (s Spork) Lookup(id uint64, name string) (*store.File, error) {
	children := s.state.ChildrenOf(id)
	for _, c := range children {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, store.ErrNoSuchFile
}

func (s Spork) Read(path string) ([]byte, error) {
	panic("implement me")
}

func (s Spork) Write(path string, offset uint64, data []byte) error {
	panic("implement me")
}

func (s Spork) Stat(path string) (store.File, error) {
	return store.File{
		Id:   uint64(time.Now().Unix()),
		Name: "some-name-right",
		Mode: 0555,
		Size: 1,
	}, nil
}
