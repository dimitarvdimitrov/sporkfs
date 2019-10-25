package fuse

import (
	"context"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
)

type handle node

func (h handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Debugf("read on id %d with handleID %d and nodeID %d", h.Id, req.Handle, req.Node)
	data, err := h.spork.Read(h.File, uint64(req.Offset), uint64(req.Size))
	if err != nil {
		return parseError(err)
	}
	resp.Data = data
	return nil
}

func (h handle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Debugf("readdirall on %s: %d", h.Name, h.Id)
	files := h.File.Children
	return toDirEnts(files), nil
}

func (h handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	log.Debugf("write on %d", req.Node)
	resp.Size, err = h.spork.Write(h.File, req.Offset, req.Data, int(req.FileFlags))
	if err != nil {
		log.Errorf("error writing: %s", err)
	}
	return parseError(err)
}

func toDirEnts(files []*store.File) []fuse.Dirent {
	dirEnts := make([]fuse.Dirent, len(files))
	for i, f := range files {
		typ := fuse.DT_File

		if f.Mode == store.ModeDirectory {
			typ = fuse.DT_Dir
		}

		dirEnts[i] = fuse.Dirent{
			Inode: f.Id,
			Type:  typ,
			Name:  f.Name,
		}
	}
	return dirEnts
}
