package inventory

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"go.uber.org/zap"
)

const (
	indexLocation = "/index"
)

func (d Driver) Name() string {
	return "inventory"
}

func (d Driver) GetState() (io.Reader, error) {
	d.m.Lock()
	defer d.m.Unlock()

	buff := &bytes.Buffer{}
	err := d.root.Serialize(buff)
	if err != nil {
		return nil, fmt.Errorf("serializing inventory root: %w", err)
	}
	return buff, nil
}

func (d Driver) SetState(r io.Reader) error {
	d.m.Lock()
	defer d.m.Unlock()

	d.root = &store.File{}
	err := d.root.Deserialize(r)
	if err != nil {
		return fmt.Errorf("setting inventory state: %w", err)
	}
	d.catalog = make(map[uint64]*store.File)
	catalogFiles(d.root, d.catalog)
	return nil
}

// persistInventory saves the file structure on disk starting with the root node
func (d Driver) persistInventory() {
	f, err := os.Create(d.location + indexLocation)
	if err != nil {
		log.Error("couldn't persist index at", zap.String("location", d.location), zap.Error(err))
	}
	defer f.Close()

	err = d.root.Serialize(f)
	if err != nil {
		log.Error("persisting storage index at", zap.String("location", d.location), zap.Error(err))
	}
}
