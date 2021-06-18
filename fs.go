package vfs

import (
	"io"
	"os"
	"strconv"
	"time"

	bbolt "go.etcd.io/bbolt"
)

type Package struct {
	db   *bbolt.DB
	data *os.File
}

func Open(path string) (*Package, error) {
	db, err := bbolt.Open(path+".index", 0777, nil)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".data", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for s := BlockSize_1K; s <= BlockSize_16M; s *= 2 {
			_, err := tx.CreateBucketIfNotExists([]byte("holes_" + strconv.Itoa(s)))
			if err != nil {
				return err
			}
		}
		_, err := tx.CreateBucketIfNotExists(trunkBucket)
		return err
	}); err != nil {
		return nil, err
	}
	return &Package{
		db:   db,
		data: f,
	}, nil
}

func (p *Package) Close() error {
	if err := p.db.Close(); err != nil {
		return err
	}
	return p.data.Close()
}

func (p *Package) getFreeBlock(sz int64) (bp BlockPos, err error) {
	assert(sz <= BlockSize_16M)
	err = p.db.Update(func(tx *bbolt.Tx) error {
		for bsz := roundSizeToBlock(sz); bsz <= BlockSize_16M; bsz *= 2 {
			bk := tx.Bucket([]byte("holes_" + strconv.FormatInt(bsz, 10)))
			k, _ := bk.Cursor().First()
			if len(k) == 8 {
				bp = unmarshalBlockPos(k)
				if err := bk.Delete(k); err != nil {
					return err
				}
				bp1, bps := bp.SplitToSize(bsz)
				bp = bp1
				for _, bp := range bps {
					if err := bp.putIntoHole(tx); err != nil {
						return err
					}
				}
				return nil
			}
		}
		// no free blocks, create one at the end of data file
		fi, err := p.data.Stat()
		if err != nil {
			return err
		}
		bp = NewBlockPos(fi.Size(), roundSizeToBlock(sz))
		return bp.putIntoHole(tx)
	})
	return
}

func (p *Package) Write(key string, value io.Reader) error {
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		if len(metabuf) == 0 {
			// m := Meta{
			// 	Name:       key,
			// 	CreateTime: time.Now().Unix(),
			// 	ModTime:    time.Now().Unix(),
			// }
		} else {
			m := unmarshalMeta(metabuf)
			m.ModTime = time.Now().Unix()
		}
		return nil
	})
}
