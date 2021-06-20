package vfs

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"

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
	return b.ForEach(func(v uint32) error { return freeBlock(tx, v) })
}

func freeBlock(tx *bbolt.Tx, v uint32) error {
	return tx.Bucket(freeBucket).Put(uint32ToBytes(v), []byte{})
}

func allocBlock(tx *bbolt.Tx, v uint32) error {
	return tx.Bucket(freeBucket).Delete(uint32ToBytes(v))
}
