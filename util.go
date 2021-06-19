package vfs

import (
	"io"
	"io/ioutil"
	"math"
	"os"
	"sync"
)

var bigBuffer = func() func() ([]byte, func()) {
	pool := sync.Pool{
		New: func() interface{} {
			return make([]byte, BlockSize_16M)
		},
	}
	return func() ([]byte, func()) {
		buf := pool.Get().([]byte)
		return buf, func() { pool.Put(buf) }
	}
}()

func roundSizeToBlock(size int64) int64 {
	assert(size <= BlockSize_16M)
	if size <= BlockSize_1K {
		return BlockSize_1K
	}
	return BlockSize_1K * int64(math.Pow(2, math.Ceil(math.Log2(float64(size)/float64(BlockSize_1K)))))
}

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
	f *os.File
}

func (r *ReadCloser) Close() error {
	return r.f.Close()
}

func (r *ReadCloser) ReadAll() ([]byte, error) {
	return ioutil.ReadAll(r)
}

func newReader(f *os.File, pos BlockPos, sz int) *Reader {
	r := &Reader{f: f, sz: sz, off: pos.Offset()}
	return r
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.off > 0 {
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
