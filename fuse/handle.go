package fuse

import (
	"context"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
)

type handle = node

func (n handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Debugf("read on id %d with handleID %d and nodeID %d", n.Id, req.Handle, req.Node)
	data, err := n.spork.Read(n.File, uint64(req.Offset), uint64(req.Size))
	if err != nil {
		return err
	}
	resp.Data = data
	return nil
}

func (n handle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Debugf("readdirall on %s: %d", n.Name, n.Id)
	files, err := n.spork.ChildrenOf(n.File)
	if err != nil {
		return nil, fuse.ENOENT
	}
	return toDirEnts(files), nil
}

func toDirEnts(files []*store.File) []fuse.Dirent {
	dirEnts := make([]fuse.Dirent, len(files))
	for i, f := range files {
		typ := fuse.DT_File

		if f.Mode == store.Directory {
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
