package inventory

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

const (
	indexLocation = "/index"
)

var root = &store.File{
	Id:   0,
	Name: "",
	Mode: store.ModeDirectory | 0666,
	Size: 1,
	Children: []*store.File{
		{
			Id:   2,
			Name: "2.txt",
			Mode: store.ModeRegularFile,
			Size: 5,
		},
		{
			Id:   3,
			Name: "3",
			Mode: store.ModeDirectory,
			Size: 1,
			Children: []*store.File{
				{
					Id:   4,
					Name: "4.txt",
					Mode: store.ModeRegularFile,
					Size: 5,
				},
				{
					Id:   5,
					Name: "5.txt",
					Mode: store.ModeRegularFile,
					Size: 5,
				},
			},
		},
	},
}

func (d Driver) Sync() {
	d.persistInventory()
}

// persistInventory saves the file structure on disk starting with the root node
func (d Driver) persistInventory() {
	f, err := os.Create(d.location + indexLocation)
	if err != nil {
		log.Errorf("couldn't persist index at %s: %w", d.location, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(d.root)
	if err != nil {
		log.Errorf("persisting storage index at %s: %w", d.location, err)
	}
}

func restoreInventory(location string) *store.File {
	f, err := os.OpenFile(location, os.O_RDONLY, store.ModeRegularFile)
	if err != nil {
		log.Fatal(fmt.Errorf("couldn't open inventory index %w", err))
	}
	defer f.Close()

	root := &store.File{}
	err = json.NewDecoder(f).Decode(root)
	if err != nil {
		log.Fatal(fmt.Errorf("couldn't read inventory index: %w", err))
	}
	return root
}
