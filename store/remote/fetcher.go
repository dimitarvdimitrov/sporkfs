package remote

import (
	"context"
	"io"

	"github.com/dimitarvdimitrov/sporkfs/api"
	proto "github.com/dimitarvdimitrov/sporkfs/api/pb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"google.golang.org/grpc"
)

const grpcBufferSize = 5 * api.ChunkSize

type Readerer interface {
	Reader(id, version uint64) (io.ReadCloser, error)
}

type grpcFetcher struct {
	client proto.FileClient
}

func newGrpcFetcher(remoteUrl string) (grpcFetcher, error) {
	conn, err := grpc.Dial(remoteUrl,
		grpc.WithInsecure(),
		grpc.WithReadBufferSize(grpcBufferSize),
	)
	if err != nil {
		return grpcFetcher{}, err
	}

	return grpcFetcher{
		client: proto.NewFileClient(conn),
	}, nil
}

func (f grpcFetcher) Reader(id, version uint64) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())

	req := &proto.ReadRequest{
		Id:      id,
		Version: version,
	}

	stream, err := f.client.Read(ctx, req, grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}

	out, in := io.Pipe()
	reader := grpcAsyncReader{
		stream:      stream,
		closeStream: cancel,
		in:          in,
		out:         out,
		done:        make(chan struct{}),
	}
	go reader.run()

	return reader, nil
}

type grpcAsyncReader struct {
	stream      proto.File_ReadClient
	closeStream func()
	done        chan struct{}
	in          *io.PipeWriter
	out         *io.PipeReader
}

func (r grpcAsyncReader) Read(p []byte) (n int, err error) {
	return r.out.Read(p)
}

func (r grpcAsyncReader) Close() error {
	close(r.done)
	r.closeStream()
	_ = r.in.Close()
	_ = r.out.Close()
	return nil
}

func (r grpcAsyncReader) run() {
	for {
		select {
		case <-r.done:
			return
		default:
		}

		reply, err := r.stream.Recv()
		if err != nil {
			_ = r.in.CloseWithError(err)
			if err != io.EOF {
				log.Errorf("reading remote file: %s", err)
			}
			return
		}

		chunk := reply.GetContent()
		_, err = r.in.Write(chunk)
		if err != nil {
			log.Warnf("couldn't receive file chunk from remote peer: %s", err)
		}
	}
}
