package remote

import (
	"context"
	"io"

	"github.com/dimitarvdimitrov/sporkfs/api"
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	"google.golang.org/grpc"
)

const bufferSize = 5 * api.ChunkSize

type Fetcher interface {
}

type grpcFetcher struct {
	client proto.FileClient
}

func NewGrpcFetcher(remoteUrl string) (*grpcFetcher, error) {
	conn, err := grpc.Dial(remoteUrl,
		grpc.WithInsecure(),
		grpc.WithReadBufferSize(bufferSize),
	)
	if err != nil {
		return nil, err
	}

	return &grpcFetcher{
		client: proto.NewFileClient(conn),
	}, nil
}

func (f grpcFetcher) Fetch(id, version uint64, offset, size int64) ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &proto.ReadRequest{
		Id:      id,
		Version: version,
		Offset:  offset,
		Size:    size,
	}

	stream, err := f.client.Read(ctx, req, grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}

	received := make([]byte, size)
	receivedLen := int64(0)

	for receivedLen < size {
		reply, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		chunk := reply.GetContent()
		copied := copy(received[receivedLen:], chunk)
		receivedLen += int64(copied)
	}

	return received, nil
}
