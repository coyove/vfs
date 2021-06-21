package vfs

import "fmt"

const (
	BlockSize = 1024 * 128
)

var (
	trunkBucket = []byte("trunk")
	freeBucket  = []byte("free")

	dataSizeKey   = []byte("*:data")
	dataFileKey   = []byte("*:datafile")
	totalSizeKey  = []byte("*:size")
	maxSizeKey    = []byte("*:maxsize")
	totalCountKey = []byte("*:count")
)

var (
	ErrAbort       = fmt.Errorf("abort loop")
	ErrInvalidName = fmt.Errorf("invalid name")
	ErrNotFound    = fmt.Errorf("not found")
)
