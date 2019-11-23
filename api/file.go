package api

import (
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	"github.com/dimitarvdimitrov/sporkfs/spork"
)

type fileServer struct {
	s spork.Spork
}

func NewFileServer(s spork.Spork) *fileServer {
	return &fileServer{
		s: s,
	}
}

func (server *fileServer) Read(req *proto.ReadRequest, stream proto.File_ReadServer) error {
	ctx := stream.Context()
	f, err := server.s.Get(req.Id)
	if err != nil {
		return err
	}

	bytes, err := server.s.Read(f, req.Offset, req.Size)
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
