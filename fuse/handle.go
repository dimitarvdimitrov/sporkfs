package fuse

import (
	"context"
	"io"
	"math/rand"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"go.uber.org/zap"
)

type handle struct {
	id   fuse.HandleID
	node node

	r spork.ReadCloser
	w spork.WriteCloser
}

func newHandle(n node, r spork.ReadCloser, w spork.WriteCloser) handle {
	hId := fuse.HandleID(rand.Uint64())

	h := handle{
		id:   hId,
		node: n,
		r:    r,
		w:    w,
	}

	return h
}

func (h handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	data := make([]byte, req.Size)
	n, err := h.r.ReadAt(data, req.Offset)
	if err != nil && err != io.EOF {
		return parseError(err)
	}
	resp.Data = data[:n]
	return nil
}

func (h handle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	files := h.node.File.Children
	return toDirEnts(files), nil
}

func (h handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	if req.FileFlags&fuse.OpenAppend != 0 {
		resp.Size, err = h.w.Write(req.Data)
	} else {
		resp.Size, err = h.w.WriteAt(req.Data, req.Offset)
	}

	return parseError(err)
}

func toDirEnts(files []*store.File) []fuse.Dirent {
	dirEnts := make([]fuse.Dirent, len(files))
	for i, f := range files {
		typ := fuse.DT_File

		if f.Mode.IsDir() {
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

func (h handle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	h.sync()
	return nil
}

func (h handle) sync() {
	if h.w != nil {
		h.w.Sync()
	}
}

func (h handle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if req.Dir {
		return nil
	}

	fId := h.node.File.Id

	if req.ReleaseFlags&fuse.ReleaseFlush != 0 {
		h.sync()
	}

	var err error
	if h.r != nil {
		if rErr := h.r.Close(); rErr != nil {
			log.Error("closing reader", log.Id(fId), zap.Error(rErr))
			err = rErr
		}
	}
	if h.w != nil {
		if wErr := h.w.Close(); wErr != nil {
			log.Error("closing writer", log.Id(fId), zap.Error(wErr))
			err = wErr
		}
	}
	return err
}
