package api

import (
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

type fileServer struct {
	data data.Driver
}

func NewFileServer(s data.Driver) *fileServer {
	return &fileServer{
		data: s,
	}
}

func (server *fileServer) Read(req *proto.ReadRequest, stream proto.File_ReadServer) error {
	ctx := stream.Context()

	bytes, err := server.data.Read(req.Id, req.Version, req.Offset, req.Size)
	if err != nil {
		return err
	}

	for len(bytes) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lenToSend := min(len(bytes), 4096)
		toSend := bytes[:lenToSend]
		msg := &proto.ReadReply{
			Content: toSend,
		}
		if err = stream.Send(msg); err != nil {
			return err
		}
		bytes = bytes[lenToSend:]
	}
	return nil
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
