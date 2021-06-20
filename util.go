package vfs

import (
	"fmt"
	"io"
	"os"
	"sync"
)

var bigBuffer = func() func() ([]byte, func()) {
	pool := sync.Pool{
		New: func() interface{} {
			return make([]byte, BlockSize)
		},
	}
	return func() ([]byte, func()) {
		buf := pool.Get().([]byte)
		return buf, func() { pool.Put(buf) }
	}
}()

func assert(v bool) {
	if !v {
		panic("assert")
	}
}

type Reader struct {
	sz   int
	f    *os.File
	read int
	off  int64
}

type ReadCloser struct {
	io.Reader
	sz  int64
	rds []io.Reader
	f   *os.File
}

func (r *ReadCloser) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
	case 1:
	case 2:
	}
	return 0, fmt.Errorf("invalid whence")
}

func (r *ReadCloser) Close() error {
	return r.f.Close()
}

func newReader(f *os.File, off int64, sz int) *Reader {
	r := &Reader{f: f, sz: sz, off: off}
	return r
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.off >= 0 {
		_, err := r.f.Seek(r.off, 0)
		if err != nil {
			return 0, err
		}
		r.off = -1
	}
	if r.read >= r.sz {
		return 0, io.EOF
	}
	n, err := r.f.Read(p)
	r.read += n
	if r.read > r.sz {
		n -= r.read - r.sz
	}
	return n, err
}
