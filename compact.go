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
	eof := bytesToInt64(tx.Bucket(trunkBucket).Get(dataSizeKey))
	c := tx.Bucket(freeBucket).Cursor()
	for k, _ := c.Last(); len(k) == 4; k, _ = c.Prev() {
		boff := bytesToUint32(k)
		h := int64(boff)*BlockSize + BlockSize
		if h == eof {
			eof -= BlockSize
			if err := deleteFromHole(tx, boff); err != nil {
				return 0, err
			}
			continue
		}
		break
	}
	return eof, tx.Bucket(trunkBucket).Put(dataSizeKey, int64ToBytes(eof))
}
