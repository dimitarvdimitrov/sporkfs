package remote

import (
	"context"
	"fmt"
	"io"
	"time"

	api "github.com/dimitarvdimitrov/sporkfs/api/file"
	proto "github.com/dimitarvdimitrov/sporkfs/api/file"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const grpcBufferSize = 5 * api.ChunkSize

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

	var stream proto.File_ReadClient
	var err error

	connected := make(chan struct{})
	go func() {
		stream, err = f.client.Read(ctx, req, grpc.WaitForReady(true))
		close(connected)
	}()

	// we need to enforce the initial connection timeout ourselves because we cant set
	// a timeout on that only without limiting the time for the actual file transfer as well
	connTimeout := time.NewTimer(time.Second / 2).C
	select {
	case <-connTimeout:
		cancel()
		return nil, fmt.Errorf("couldn't connect to peer")
	case <-connected:
	}

	_, err = stream.Recv() // the server will send a dummy reply first to confirm it has the file
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
				log.Error("reading remote file", zap.Error(err))
			}
			return
		}

		_, err = r.in.Write(reply.Content)
		if err != nil {
			log.Warn("couldn't receive file chunk from remote peer", zap.Error(err))
		}
	}
}
