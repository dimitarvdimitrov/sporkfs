package fuse

import (
	"context"
	"io"
	"math/rand"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
)

type handle struct {
	node

	r     spork.Reader
	w     spork.Writer
	fsync chan struct{}
}

func newHandle(n node, r spork.Reader, w spork.Writer) (handle, fuse.HandleID) {
	fsync := make(chan struct{})
	hId := fuse.HandleID(rand.Uint64())

	fsyncReqM.Lock()
	if chans := fsyncReq[n.Id]; chans == nil {
		fsyncReq[n.Id] = make(map[fuse.HandleID]chan struct{}, 1)
	}
	fsyncReq[n.Id][hId] = fsync
	fsyncReqM.Unlock()

	h := handle{
		node:  n,
		r:     r,
		w:     w,
		fsync: fsync,
	}
	go h.run()

	return h, hId
}

func (h handle) run() {
	for range h.fsync {
		h.flush()
	}
}

func (h handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	data, err := h.spork.Read(h.File, req.Offset, int64(req.Size))
	if err != nil {
		return parseError(err)
	}
	resp.Data = data
	return nil
}

func (h handle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	files := h.File.Children
	return toDirEnts(files), nil
}

func (h handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	resp.Size, err = h.w.WriteAt(req.Data, req.Offset)
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

func (h handle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	h.flush()
	return nil
}

func (h handle) flush() {
	if h.w != nil {
		h.w.Flush()
	}
}

func (h handle) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	fsyncReqM.Lock()
	close(h.fsync)
	delete(fsyncReq[h.Id], req.Handle)
	fsyncReqM.Unlock()

	if h.r != nil {
		if rErr := h.r.Close(); rErr != nil {
			log.Errorf("closing reader %x: %s", h.File.Id, rErr)
			err = rErr
		}
	}
	if h.w != nil {
		if wErr := h.w.Close(); wErr != nil {
			log.Errorf("closing writer %x: %s", h.File.Id, wErr)
			err = wErr
		}
	}
	return err
}
