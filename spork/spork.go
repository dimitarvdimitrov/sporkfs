package spork

import (
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/state"
)

// TODO remove this
var S = Spork{
	state: state.NewDriver(""),
	data:  data.NewLocalDriver("/opt/storage/data"),
}

type Spork struct {
	state       state.Driver
	data, cache data.Driver
}

func (s Spork) Root() *store.File {
	s.data.Sync()
	return s.state.Root()
}

func (s Spork) Lookup(f *store.File, name string) (*store.File, error) {
	for _, c := range f.Children {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, store.ErrNoSuchFile
}

func (s Spork) ReadAll(f *store.File) ([]byte, error) {
	return s.Read(f, 0, f.Size)
}

func (s Spork) Read(f *store.File, offset, size uint64) ([]byte, error) {
	return s.data.Read(f, offset, size)
}

func (s Spork) Write(f *store.File, offset uint64, data []byte) error {
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
