package vfs

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

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

type Meta struct {
	Name       string     `json:"n"`
	Size       int64      `json:"sz"`
	Positions  []BlockPos `json:"pos"`
	CreateTime int64      `json:"ct"`
	ModTime    int64      `json:"mt"`
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

type BlockPos uint64

func NewBlockPos(offset int64, size int64) BlockPos {
	x := int64(math.Log2(float64(size / BlockSize_1K)))
	return BlockPos(offset)<<8 | BlockPos(x)
}

func unmarshalBlockPos(b []byte) BlockPos {
	return BlockPos(binary.BigEndian.Uint64(b))
}

func (bp BlockPos) Offset() (offset int64) {
	return int64(bp) >> 8
}

func (bp BlockPos) Start() (offset int64) {
	return int64(bp) >> 8
}

func (bp BlockPos) Size() (size int64) {
	return BlockSize_1K * int64(math.Pow(2, float64(byte(bp))))
}

func (bp BlockPos) End() (size int64) {
	return bp.Offset() + bp.Size()
}

func (bp BlockPos) Split() (BlockPos, BlockPos) {
	assert(bp.Size() > BlockSize_1K)
	sz := bp.Size() / 2
	return NewBlockPos(bp.Offset(), sz), NewBlockPos(bp.Offset()+sz, sz)
}

func (bp BlockPos) SplitToSize(size int64) (BlockPos, []BlockPos) {
	assert(size <= bp.Size())
	if size == bp.Size() {
		return bp, nil
	}
	bps := []BlockPos{}
	for {
		bp1, bp2 := bp.Split()
		bps = append(bps, bp2)
		bp = bp1
		if bp.Size() == size {
			break
		}
	}
	return bp, bps
}

func (bp BlockPos) putIntoHole(tx *bbolt.Tx) error {
	return tx.Bucket([]byte("holes_"+strconv.FormatInt(bp.Size(), 10))).Put(bp.marshal(), []byte("1"))
}

func (bp BlockPos) deleteFromHole(tx *bbolt.Tx) error {
	return tx.Bucket([]byte("holes_" + strconv.FormatInt(bp.Size(), 10))).Delete(bp.marshal())
}

func (bp BlockPos) marshal() []byte {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(bp))
	return b[:]
}

func (bp BlockPos) String() string {
	sz := [...]string{"1K", "2K", "4K", "8K", "16K", "32K", "64K", "128K", "256K", "512K", "1M", "2M", "4M", "8M", "16M"}[int64(math.Log2(float64(bp.Size()/BlockSize_1K)))]
	return fmt.Sprintf("%d-%d(%s)", bp.Offset(), bp.End(), sz)
}
