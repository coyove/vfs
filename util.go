package vfs

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
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

func random(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(rand.Int())
	}
	return buf
}

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

func (f *File) Size() int64 {
	return f.size
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

func checkName(s string) bool {
	valid := func(s string) bool {
		if s == "" || s == "/" {
			return false
		}
		dots, size := 0, 0
		for _, r := range s {
			size++
			switch r {
			case '/', '*', '?', '\\', ':', '"', '<', '>', '|':
				return false
			case '.':
				dots++
			}
		}
		if dots == size {
			return false
		}
		return true
	}
	s = strings.TrimPrefix(s, "/")
	for _, p := range strings.Split(s, "/") {
		if !valid(p) {
			return false
		}
	}
	return true
}

func kvsToMap(kvs ...string) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	m := map[string]string{}
	for i := 0; i < len(kvs); i += 2 {
		m[kvs[i]] = kvs[i+1]
	}
	return m
}
