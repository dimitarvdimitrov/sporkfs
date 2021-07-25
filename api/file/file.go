package file

import (
	"io"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

// ChunkSize is the size of the chunk of file
// that is sent over in stream messages of File.Read()
const ChunkSize = 1 << 16

type fileServer struct {
	data, cache data.Driver
}

func NewFileServer(s, c data.Driver) *fileServer {
	return &fileServer{
		data:  s,
		cache: c,
	}
}

func (server *fileServer) Read(req *ReadRequest, stream File_ReadServer) error {
	log.Debug("[file_api] received read grpc request", log.Id(req.Id), log.Ver(req.Version))
	defer log.Debug("[file_api] returned read grpc request", log.Id(req.Id), log.Ver(req.Version))

	var src data.Driver
	if server.cache.Contains(req.Id, req.Version) {
		src = server.cache
	} else if server.data.Contains(req.Id, req.Version) {
		src = server.data
	} else {
		log.Debug("[file server] file not known", log.Id(req.Id), log.Ver(req.Version))
		return store.ErrNoSuchFile
	}

	reader, err := src.Reader(req.Id, req.Version, os.O_RDONLY)
	if err != nil {
		return err
	}

	// send an empty reply to confirm we have the file
	if err = stream.Send(&ReadReply{}); err != nil {
		return err
	}

	off := int64(0)
	buff := make([]byte, ChunkSize, ChunkSize)

	for {
		n, err := reader.ReadAt(buff, off)
		if err != nil && err != io.EOF {
			return err
		}
		if n < 1 {
			break
		}

		msg := &ReadReply{
			Content: buff[:n],
		}
		if err = stream.Send(msg); err != nil {
			return err
		}
		off += int64(n)
		buff = buff[:cap(buff)] // reset the buffer
	}
	return nil
}
