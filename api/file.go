package api

import (
	"io"
	"os"

	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

// ChunkSize is the size of the chunk of file
// that is sent over in stream messages of File.Read()
const ChunkSize = 1 << 16

type fileServer struct {
	data data.Driver
}

func NewFileServer(s data.Driver) *fileServer {
	return &fileServer{
		data: s,
	}
}

func (server *fileServer) Read(req *proto.ReadRequest, stream proto.File_ReadServer) error {
	log.Debugf("received request for %d-%d", req.Id, req.Version)
	reader, err := server.data.Reader(req.Id, req.Version, os.O_RDONLY)
	if err != nil {
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

		msg := &proto.ReadReply{
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
