package spork

import "io"

type ReadWriteCloser interface {
	Reader
	Writer
	io.Closer
}

type readWriter struct {
	r ReadCloser
	w WriteCloser
}

func (r readWriter) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r readWriter) ReadAt(p []byte, off int64) (n int, err error) {
	return r.r.ReadAt(p, off)
}

func (r readWriter) Sync() {
	r.w.Sync()
}

func (r readWriter) WriteAt(p []byte, off int64) (n int, err error) {
	return r.w.WriteAt(p, off)
}

func (r readWriter) Write(p []byte) (n int, err error) {
	return r.w.Write(p)
}

func (r readWriter) Close() (err error) {
	if wErr := r.w.Close(); wErr != nil {
		err = wErr
	}

	if rErr := r.r.Close(); rErr != nil {
		err = rErr
	}
	return
}
