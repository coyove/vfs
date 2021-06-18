package vfs

import "testing"

func TestConst(t *testing.T) {
	bp := NewBlockPos(10, BlockSize_16M)
	t.Log(bp.SplitToSize(BlockSize_1K * 4))
}
