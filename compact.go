package vfs

import (
	bbolt "go.etcd.io/bbolt"
)

func (p *Package) Compact() (err error) {
	eof := int64(0)
	err = p.db.Update(func(tx *bbolt.Tx) error {
		eof, err = p.calcTruncate(tx)
		return err
	})
	if err != nil {
		return err
	}
	return p.data.Truncate(eof)
}

func (p *Package) calcTruncate(tx *bbolt.Tx) (int64, error) {
	bk := tx.Bucket(trunkBucket)
	eof := bytesToInt64(bk.Get(dataSizeKey))
	x := FreeBitmap(bk.Get(freeKey))
	// c.cursor = len(c.src) - 1
	// for {
	// 	v, ok := c.FindPrev()
	// 	if !ok {
	// 		break
	// 	}
	// 	if v*BlockSize+BlockSize == eof {
	// 		eof--
	// 	}
	// }
	return eof, bk.Put(dataSizeKey, int64ToBytes(eof))
}
