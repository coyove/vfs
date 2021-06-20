package vfs

const (
	BlockSize = 1024 * 128
)

var (
	trunkBucket   = []byte("trunk")
	freeBucket    = []byte("free")
	dataSizeKey   = []byte("*:data")
	totalSizeKey  = []byte("*:size")
	maxSizeKey    = []byte("*:maxsize")
	totalCountKey = []byte("*:count")
)
