package spork

import (
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"github.com/dimitarvdimitrov/sporkfs/store/inventory"
)

type Spork struct {
	inventory   inventory.Driver
	data, cache data.Driver
}

func New(c Config) Spork {
	return Spork{
		inventory: inventory.NewDriver(c.InventoryLocation),
		data:      data.NewLocalDriver(c.DataLocation),
	}
}

func (s Spork) Root() *store.File {
	return s.inventory.Root()
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
	f.RLock()
	defer f.RUnlock()

	return s.Read(f, 0, f.Size)
}

func (s Spork) Read(f *store.File, offset, size uint64) ([]byte, error) {
	f.RLock()
	defer f.RUnlock()

	return s.data.Read(f, offset, size)
}

func (s Spork) Write(f *store.File, offset int64, data []byte, flags int) (int, error) {
	f.Lock()
	defer f.Unlock()
	defer func() {
		f.Size = uint64(s.data.Size(f))
	}()

	return s.data.Write(f, offset, data, flags)
}

func (s Spork) CreateFile(parent *store.File, name string, mode store.FileMode) (*store.File, error) {
	parent.Lock()
	defer parent.Unlock()

	f := s.newFile(name, mode)
	f.Lock()
	defer f.Unlock()

	err := s.inventory.Add(f)
	if err != nil {
		return nil, err
	}

	err = s.data.Add(f)
	if err != nil {
		s.inventory.Remove(f.Id)
		return nil, err
	}

	parent.Children = append(parent.Children, f)
	return f, nil
}

func (s Spork) newFile(name string, mode store.FileMode) *store.File {
	return &store.File{
		RWMutex:  &sync.RWMutex{},
		Id:       s.inventory.NewId(),
		Name:     name,
		Mode:     mode,
		Size:     0,
		Children: nil,
	}
}

func (s Spork) Rename(file, oldParent, newParent *store.File, newName string) error {
	oldParent.Lock()
	defer oldParent.Unlock()

	file.Lock()
	defer file.Unlock()

	file.Name = newName

	if oldParent.Id != newParent.Id {
		newParent.Lock()
		defer newParent.Unlock()

		for i, c := range oldParent.Children {
			if c.Id == file.Id {
				oldParent.Children = append(oldParent.Children[:i], oldParent.Children[i+1:]...)
				break
			}
		}

		newParent.Children = append(newParent.Children, file)
	}

	return nil
}

func (s Spork) Delete(file, parent *store.File) error {
	if len(file.Children) != 0 {
		return store.ErrDirectoryNotEmpty
	}

	file.Lock()
	defer file.Unlock()

	parent.Lock()
	defer parent.Unlock()

	found := false
	for i, c := range parent.Children {
		if c.Id == file.Id {
			s.data.Remove(file.Id)
			s.inventory.Remove(file.Id)
			parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)

			found = true
			break
		}
	}

	if !found {
		return store.ErrNoSuchFile
	}

	return nil
}

//func (s Spork) Link(file, newParent *store.File, name string) *store.File {
//	file.RLock()
//	defer file.RUnlock()
//
//	newParent.Lock()
//	defer newParent.Unlock()
//
//	newFile := file.Copy()
//	newFile.Name = name
//	newParent.Children = append(newParent.Children, newFile)
//
//	return newFile
//}

func (s Spork) Close() {
	s.data.Sync()
	s.inventory.Sync()
}
