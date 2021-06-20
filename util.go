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

type File struct {
	f       *os.File
	size    int64
	offsets []int64
	cursor  int64
	small   []byte
}

func (r *File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		r.cursor = offset
	case 1:
		r.cursor += offset
	case 2:
		r.cursor = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	if r.cursor < 0 || r.cursor >= r.size {
		return 0, fmt.Errorf("invalid cursor: %v", r.cursor)
	}
	return r.cursor, nil
}

func (r *File) Close() error {
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}

func (r *File) Read(p []byte) (int, error) {
	if r.cursor >= r.size {
		return 0, io.EOF
	}

	if r.f == nil { // use r.small
		n := copy(p, r.small[r.cursor:])
		r.cursor += int64(n)
		return n, nil
	}

	idx := r.cursor / BlockSize
	assert(int(idx) < len(r.offsets))

	_, err := r.f.Seek(r.offsets[idx]+r.cursor-idx*BlockSize, 0)
	if err != nil {
		return 0, err
	}

	cursorInBlock := r.cursor - r.cursor/BlockSize*BlockSize
	var left int64
	if int(idx) == len(r.offsets)-1 {
		lastBlockSize := r.size % BlockSize
		if lastBlockSize == 0 {
			lastBlockSize = BlockSize
		}
		left = lastBlockSize - cursorInBlock
	} else {
		left = BlockSize - cursorInBlock
	}

	if len(p) > int(left) {
		p = p[:left]
	}
	n, err := r.f.Read(p)
	r.cursor += int64(n)
	return n, err
}
