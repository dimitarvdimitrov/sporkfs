package raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"go.uber.org/zap"

	"github.com/dimitarvdimitrov/sporkfs/log"
)

type marshallable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type marshallableState struct {
	name string
	c    marshallable
}

func (c marshallableState) Name() string {
	return c.name
}

func (c marshallableState) GetState() (io.Reader, error) {
	stateBytes, err := c.c.Marshal()
	if err != nil {
		log.Error("couldn't marshall conf state", zap.Error(err))
		return nil, err
	}
	return bytes.NewReader(stateBytes), nil
}

func (c marshallableState) SetState(r io.Reader) error {
	stateBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("settign conf state state: %w", err)
	}
	return c.c.Unmarshal(stateBytes)
}
