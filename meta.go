package vfs

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

func bytesToInt64(p []byte) int64 {
	if len(p) == 0 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(p))
}

func int64ToBytes(v int64) []byte {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func bytesToUint32(p []byte) uint32 {
	if len(p) == 0 {
		return 0
	}
	return binary.BigEndian.Uint32(p)
}

func uint32ToBytes(v uint32) []byte {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

type Dir struct {
	Name string
}

func (d Dir) String() string {
	return fmt.Sprintf("<%q>", d.Name)
}

type Meta struct {
	Name       string            `json:"n"`
	Size       int64             `json:"sz"`
	Positions  Blocks            `json:"pos"`
	CreateTime int64             `json:"ct"`
	ModTime    int64             `json:"mt"`
	SmallData  []byte            `json:"R"`
	Sha1       [sha1.Size]byte   `json:"S"`
	Tags       map[string]string `json:"T"`
}

func unmarshalMeta(p []byte) Meta {
	m := Meta{}
	json.Unmarshal(p, &m)
	return m
}

func (m Meta) marshal() []byte {
	buf, _ := json.Marshal(m)
	return buf
}

func (m Meta) String() string {
	return fmt.Sprintf("<%q-%d-%016x-%v-%v-%v>", m.Name, m.Size, m.Sha1[:8], m.Tags,
		time.Unix(m.CreateTime, 0).Format(time.ANSIC),
		time.Unix(m.ModTime, 0).Format(time.ANSIC),
	)
}

type Blocks []byte

func (b *Blocks) Append(v uint32) {
	*b = append(*b, 0, 0, 0, 0, 0)
	n := binary.PutUvarint((*b)[len(*b)-5:], uint64(v))
	*b = (*b)[:len(*b)-5+n]
}

func (b Blocks) ForEach(f func(v uint32) error) error {
	for x := b; len(x) > 0; {
		v, n := binary.Uvarint(x)
		if n == 0 {
			break
		}
		assert(n > 0)
		if err := f(uint32(v)); err != nil {
			return err
		}
		x = x[n:]
	}
	return nil
}

func (b Blocks) String() string {
	buf := make([]string, 0, len(b)/2)
	b.ForEach(func(v uint32) error {
		buf = append(buf, "0x"+strconv.FormatInt(int64(v)*BlockSize, 16))
		return nil
	})
	return "[" + strings.Join(buf, ",") + "]"
}

func (b Blocks) Free(tx *bbolt.Tx) error {
	trunk := tx.Bucket(trunkBucket)
	m := FreeBitmap(append([]byte{}, trunk.Get(freeKey)...))
	b.ForEach(func(v uint32) error {
		m.Free(v)
		return nil
	})
	return trunk.Put(freeKey, m)
}

type FreeBitmap []byte

func (b *FreeBitmap) Free(v uint32) {
	idx := int(v / 8)
	for len(*b) <= idx {
		*b = append(*b, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"...)
	}
	(*b)[idx] &^= 1 << (v % 8)
}

type FreeBitmapCursor struct {
	src    FreeBitmap
	cursor int
}

func (f *FreeBitmapCursor) Next() (uint32, bool) {
	for {
		idx := f.cursor / 8
		if idx >= len(f.src) {
			return 0, false
		}
		if (f.src[idx]>>(f.cursor%8))&1 == 0 {
			f.src[idx] |= 1 << (f.cursor % 8)
			f.cursor++
			return uint32(f.cursor) - 1, true
		}
		f.cursor++
	}
}
