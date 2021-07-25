package inventory

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dimitarvdimitrov/sporkfs/store"
)

func (d *Driver) Name() string {
	return "inventory"
}

func (d *Driver) GetState() (io.Reader, error) {
	d.m.Lock()
	defer d.m.Unlock()

	buff := &bytes.Buffer{}
	err := d.root.Serialize(buff)
	if err != nil {
		return nil, fmt.Errorf("serializing inventory root: %w", err)
	}
	return buff, nil
}

func (d *Driver) SetState(r io.Reader) error {
	d.m.Lock()
	defer d.m.Unlock()

	d.root = &store.File{}
	err := d.root.Deserialize(r)
	if err != nil {
		return fmt.Errorf("setting inventory state: %w", err)
	}
	d.catalog = make(map[uint64][]*store.File)
	catalogFiles(d.root, d.catalog)
	return nil
}
