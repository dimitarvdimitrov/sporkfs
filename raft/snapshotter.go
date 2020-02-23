package raft

import (
	"archive/zip"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/dimitarvdimitrov/sporkfs/log"
)

// StateSource is an interface that may be implemented by sources of state for RAFT.
type StateSource interface {
	Name() string

	// GetState should return a reader to a copy of the current state. If the state changes, after the call
	// to GetState, the Read() behaviour of the returned io.Reader shouldn't change.
	GetState() (io.Reader, error)
	SetState(io.Reader) error
}

// snapshotter creates and recovers snapshots to and from the states
type snapshotter struct {
	states map[string]StateSource // map from name of StateSource to StateSource
}

func newSnapshotter(states ...StateSource) *snapshotter {
	s := &snapshotter{states: make(map[string]StateSource, len(states))}

	for _, state := range states {
		stateName := state.Name()
		if _, ok := s.states[stateName]; ok {
			log.Panic("multiple state with the same name", zap.String("name", stateName))
		}
		s.states[stateName] = state
	}
	return s
}

func (s *snapshotter) createSnap(w io.Writer) error {
	log.Debug("creating raft snapshot")
	defer log.Debug("finished creating raft snapshot")

	snapFile := zip.NewWriter(w)
	defer snapFile.Close()

	for name, p := range s.states {
		state, err := p.GetState()
		if err != nil {
			return err
		}
		if err := zipState(snapFile, state, name); err != nil {
			return err
		}
	}
	return nil
}

func zipState(z *zip.Writer, src io.Reader, key string) error {
	w, err := z.Create(key)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, src)
	return err
}

// readSnap returns a snapshotter that has all the States populated by the snapshot read from r. size is the number
// of bytes in r.
func (s *snapshotter) recoverSnap(r io.ReaderAt, size int64) error {
	snapFile, err := zip.NewReader(r, size)
	if err != nil {
		log.Error("couldn't read snapshot archive", zap.Error(err))
		return fmt.Errorf("read snapshot: %w", err)
	}

	for _, f := range snapFile.File {
		state, ok := s.states[f.Name]
		if !ok {
			return fmt.Errorf("state in snapshot not amtching any known state source; state in snapshot:%s", f.Name)
		}

		r, err := f.Open()
		if err != nil {
			return fmt.Errorf("opening snapshot part(%s): %w", f.Name, err)
		}
		err = state.SetState(r)
		_ = r.Close()
		if err != nil {
			return fmt.Errorf("setting state on %s: %w", f.Name, err)
		}
	}
	return nil
}
