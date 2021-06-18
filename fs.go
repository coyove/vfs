package vfs

import (
	"io"
	"os"
	"strconv"

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

func (p *Package) Write(key string, value io.Reader) error {
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		meta := bk.Get(keybuf)
		if len(meta) == 0 {
		} else {
		}
		return nil
	})
}
