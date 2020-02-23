package store

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

type FileMode = os.FileMode

const (
	ModeDirectory   FileMode = os.ModeDir
	ModeRegularFile FileMode = 0666
)

type File struct {
	*sync.RWMutex

	Id      uint64
	Name    string
	Mode    FileMode
	Size    int64
	Version uint64
	Atime   time.Time
	Mtime   time.Time

	Parent   *File
	Children []*File
}

type jsonFile File

func (f *File) Serialize(w io.Writer) error {
	return json.NewEncoder(w).Encode(f)
}

func (f *File) MarshalJSON() ([]byte, error) {
	p := f.Parent
	f.Parent = nil
	b, err := json.Marshal(jsonFile(*f))
	f.Parent = p

	return b, err
}

func (f *File) Deserialize(r io.Reader) error {
	return json.NewDecoder(r).Decode(f)
}

func (f *File) UnmarshalJSON(b []byte) error {
	jf := &jsonFile{}
	err := json.Unmarshal(b, jf)
	*f = File(*jf)

	f.RWMutex = &sync.RWMutex{}
	for _, c := range f.Children {
		c.Parent = f
	}
	return err
}
