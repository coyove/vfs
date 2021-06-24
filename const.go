package vfs

import "fmt"

const (
	BlockSize      = 1024 * 128
	SmallBlockSize = 1024 * 2
)

var (
	trunkBucket = []byte("trunk")

	dataSizeKey   = []byte("*:data")
	dataFileKey   = []byte("*:datafile")
	totalSizeKey  = []byte("*:size")
	totalCountKey = []byte("*:count")
	freeKey       = []byte("*:free")
)

var (
	ErrAbort       = fmt.Errorf("abort loop")
	ErrInvalidName = fmt.Errorf("invalid name")
	ErrNotFound    = fmt.Errorf("not found")
)
