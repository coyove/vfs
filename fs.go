package vfs

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	bbolt "go.etcd.io/bbolt"
)

type Package struct {
	db   *bbolt.DB
	data *os.File
}

func Open(path string) (*Package, error) {
	path = strings.TrimSuffix(path, ".index")
	path = strings.TrimSuffix(path, ".data")
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
	p := &Package{
		db:   db,
		data: f,
	}
	return p, p.Compact()
}

func (p *Package) Close() error {
	if err := p.db.Close(); err != nil {
		return err
	}
	return p.data.Close()
}

func (p *Package) writeData(buf []byte, bp BlockPos, padSize bool) error {
	_, err := p.data.Seek(bp.Offset(), 0)
	if err != nil {
		return err
	}
	n, err := p.data.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	if !padSize {
		return nil
	}
	if n == int(bp.Size()) {
		return nil
	}
	paddings := make([]byte, bp.Size()-int64(n))
	n, err = p.data.Write(paddings)
	if err != nil {
		return err
	}
	if n != len(paddings) {
		return io.ErrShortWrite
	}
	return nil
}

func (p *Package) putData(tx *bbolt.Tx, buf []byte) (BlockPos, error) {
	sz := int64(len(buf))
	assert(sz <= BlockSize_16M)
	for bsz := roundSizeToBlock(sz); bsz <= BlockSize_16M; bsz *= 2 {
		bk := tx.Bucket([]byte("holes_" + strconv.FormatInt(bsz, 10)))
		k, _ := bk.Cursor().First()
		if len(k) == 8 {
			bp := unmarshalBlockPos(k)
			if err := bk.Delete(k); err != nil {
				return 0, err
			}
			bp1, bps := bp.SplitToSize(bsz)
			bp = bp1
			for _, bp := range bps {
				if err := bp.putIntoHole(tx); err != nil {
					return 0, err
				}
			}
			return bp, p.writeData(buf, bp, false)
		}
	}
	// No free blocks, create one at the end of data file
	fi, err := p.data.Seek(0, 2)
	if err != nil {
		return 0, err
	}
	bp := NewBlockPos(fi, roundSizeToBlock(sz))
	return bp, p.writeData(buf, bp, true)
}

func (p *Package) ReadAll(key string) ([]byte, error) {
	r, err := p.Read(key)
	if err != nil {
		return nil, err
	}
	return r.ReadAll()
}

func (p *Package) Read(key string) (*ReadCloser, error) {
	var m Meta
	keybuf := []byte(key)
	err := p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		if len(metabuf) == 0 {
			return fmt.Errorf("not found")
		}
		m = unmarshalMeta(metabuf)
		return nil
	})
	if err != nil {
		return nil, err
	}
	readers := []io.Reader{}
	f, err := os.OpenFile(p.data.Name(), os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	total := int64(0)
	for _, bp := range m.Positions {
		// fmt.Println("read", bp)
		total += bp.Size()
		if total > m.Size {
			readers = append(readers, newReader(f, bp, int(m.Size-(total-bp.Size()))))
		} else {
			readers = append(readers, newReader(f, bp, int(bp.Size())))
		}
	}
	return &ReadCloser{io.MultiReader(readers...), f}, nil
}

func (p *Package) Write(key string, value io.Reader) error {
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		m := Meta{
			Name:       key,
			CreateTime: time.Now().Unix(),
			ModTime:    time.Now().Unix(),
		}

		if len(metabuf) > 0 {
			mm := unmarshalMeta(metabuf)
			m.CreateTime = mm.CreateTime
			for _, bp := range mm.Positions {
				if err := bp.putIntoHole(tx); err != nil {
					return err
				}
			}
			if err := p.incTotalSize(tx, -mm.Size, -1); err != nil {
				return err
			}
			defer p.compactHoles(tx)
		}

		buf, clean := bigBuffer()
		defer clean()
		for {
			n, err := value.Read(buf)
			if n > 0 {
				m.Size += int64(n)
				bp, err := p.putData(tx, buf[:n])
				if err != nil {
					return err
				}
				m.Positions = append(m.Positions, bp)
			}
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}

		if err := p.incTotalSize(tx, m.Size, 1); err != nil {
			return err
		}
		// fmt.Println(m.Positions)
		return bk.Put(keybuf, m.marshal())
	})
}

func (p *Package) Delete(key string) error {
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		if len(metabuf) == 0 {
			return nil
		}
		m := unmarshalMeta(metabuf)
		for _, bp := range m.Positions {
			if err := bp.putIntoHole(tx); err != nil {
				return err
			}
		}
		if err := p.compactHoles(tx); err != nil {
			return err
		}
		if err := p.incTotalSize(tx, -m.Size, -1); err != nil {
			return err
		}
		return bk.Delete(keybuf)
	})
}

func (p *Package) Compact() error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		return p.compactHoles(tx)
	})
}

func (p *Package) compactHoles(tx *bbolt.Tx) error {
	for s := BlockSize_1K; s < BlockSize_16M; s *= 2 {
		c := tx.Bucket([]byte("holes_" + strconv.Itoa(s))).Cursor()
		holes := []BlockPos{}
		for k, _ := c.First(); len(k) == 8; k, _ = c.Next() {
			holes = append(holes, unmarshalBlockPos(k))
		}
		if len(holes) < 2 {
			return nil
		}
		for i := 0; i < len(holes)-1; {
			a, b := holes[i], holes[i+1]
			if a.Size() == b.Size() && a.End() == b.Offset() {
				c := NewBlockPos(a.Offset(), a.Size()*2)
				if err := c.putIntoHole(tx); err != nil {
					return err
				}
				if err := a.deleteFromHole(tx); err != nil {
					return err
				}
				if err := b.deleteFromHole(tx); err != nil {
					return err
				}
				i += 2
			} else {
				i++
			}
		}
	}
	return nil
}

func (p *Package) incTotalSize(tx *bbolt.Tx, sz, cnt int64) error {
	bk := tx.Bucket(trunkBucket)
	if sz != 0 {
		if err := bk.Put(totalSizeKey, int64ToBytes(bytesToInt64(bk.Get(totalSizeKey))+sz)); err != nil {
			return err
		}
	}
	if cnt != 0 {
		if err := bk.Put(totalCountKey, int64ToBytes(bytesToInt64(bk.Get(totalCountKey))+cnt)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Package) Stat() (totalSize int64, totalCount int64, holes map[string]int64) {
	p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		totalSize = bytesToInt64(bk.Get(totalSizeKey))
		totalCount = bytesToInt64(bk.Get(totalCountKey))
		holes = map[string]int64{}
		for s := BlockSize_1K; s <= BlockSize_16M; s *= 2 {
			c := tx.Bucket([]byte("holes_" + strconv.Itoa(s))).Cursor()
			x := int64(0)
			k, _ := c.First()
			last := ""
			for ; len(k) == 8; k, _ = c.Next() {
				last = unmarshalBlockPos(k).String()
				x++
			}
			if last != "" {
				holes[last] = x
			}
		}
		return nil
	})
	return
}
