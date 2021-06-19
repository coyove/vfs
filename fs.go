package vfs

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	bbolt "go.etcd.io/bbolt"
)

var testFlagSimulateDataWriteError = 0

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
	if _, err := p.data.Seek(bp.Offset(), 0); err != nil {
		return err
	}

	if testFlagSimulateDataWriteError > 0 && rand.Intn(testFlagSimulateDataWriteError) == 0 {
		x := buf[:rand.Intn(len(buf))]
		fmt.Println("test flag: simulate data write error", bp, "size=", len(buf), "write=", len(x))
		p.data.Write(x)
		return fmt.Errorf("testable")
	}

	n, err := p.data.Write(buf)
	if err != nil || n != len(buf) {
		return fmt.Errorf("write data: %v, written: %v", err, n)
	}

	if padSize && n < int(bp.Size()) {
		paddings := make([]byte, bp.Size()-int64(n))
		n, err = p.data.Write(paddings)
		if err != nil || n != len(paddings) {
			return fmt.Errorf("write paddings: %v, written: %v", err, n)
		}
	}
	return nil
}

func (p *Package) putData(tx *bbolt.Tx, buf []byte) ([]BlockPos, error) {
	sz := int64(len(buf))
	downSearched := false
	assert(sz <= BlockSize_16M)
	// Search upwards for free blocks
	for bsz := roundSizeToBlock(sz); bsz <= BlockSize_16M; bsz *= 2 {
		bk := tx.Bucket([]byte("holes_" + strconv.FormatInt(bsz, 10)))
		k, _ := bk.Cursor().First()
		if len(k) == 8 {
			bp := unmarshalBlockPos(k)
			if err := bk.Delete(k); err != nil {
				return nil, err
			}
			bp1, bps := bp.SplitToSize(bsz)
			bp = bp1
			for _, bp := range bps {
				if err := bp.putIntoHole(tx); err != nil {
					return nil, err
				}
			}
			return []BlockPos{bp}, p.writeData(buf, bp, false)
		}
		// Search downwards if it is a relatively big block (>1M)
		if !downSearched {
			if bsz := roundSizeToBlock(sz); bsz > BlockSize_1K*1024 {
				bsz /= 2
				bk := tx.Bucket([]byte("holes_" + strconv.FormatInt(bsz, 10)))
				if bk.Stats().KeyN >= 1 {
					a, err := p.putData(tx, buf[:bsz])
					if err != nil {
						return nil, err
					}
					b, err := p.putData(tx, buf[bsz:])
					if err != nil {
						return nil, err
					}
					return append(a, b...), nil
				}
			}
			downSearched = true
		}
	}
	// No free blocks, create one at the end of data file
	fi, err := p.data.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	bp := NewBlockPos(fi, roundSizeToBlock(sz))
	return []BlockPos{bp}, p.writeData(buf, bp, true)
}

func (p *Package) ReadAll(key string) ([]byte, error) {
	r, err := p.Read(key)
	if err != nil {
		return nil, err
	}
	return r.ReadAll()
}

func (p *Package) Meta(key string) (Meta, error) {
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
	return m, err
}

func (p *Package) Read(key string) (*ReadCloser, error) {
	m, err := p.Meta(key)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(p.data.Name(), os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	total := int64(0)
	readers := make([]io.Reader, 0, len(m.Positions))
	for _, bp := range m.Positions {
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
	return p.db.Update(func(tx *bbolt.Tx) (E error) {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		m := Meta{
			Name:       key,
			CreateTime: time.Now().Unix(),
			ModTime:    time.Now().Unix(),
		}

		if len(metabuf) > 0 {
			// Overwrite existing data
			mm := unmarshalMeta(metabuf)
			m.CreateTime = mm.CreateTime
			if err := p.incTotalSize(tx, -mm.Size, -1); err != nil {
				return err
			}
			defer func() {
				if E == nil {
					for _, bp := range mm.Positions {
						if err := bp.putIntoHole(tx); err != nil {
							E = err
							return
						}
					}
					if rand.Intn(4) == 0 {
						E = p.compactHoles(tx)
					}
				}
			}()
			// Write data to new blocks, then recycle old blocks in the above defer-call
		}

		beforeSize, err := p.data.Seek(0, 2)
		if err != nil {
			return err
		}

		buf, clean := bigBuffer()
		defer func() {
			clean()
			if E != nil {
				p.data.Truncate(beforeSize)
			}
		}()

		for {
			n, err := value.Read(buf)
			if n > 0 {
				m.Size += int64(n)
				bp, err := p.putData(tx, buf[:n])
				if err != nil {
					return err
				}
				m.Positions = append(m.Positions, bp...)
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

func (p *Package) Rename(oldname, newname string) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		m, _ := p.Meta(newname)
		if m.Name == newname {
			return fmt.Errorf("rename: new name already occupied")
		}

		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get([]byte(oldname))
		if len(metabuf) == 0 {
			return nil
		}
		m = unmarshalMeta(metabuf)
		m.Name = newname
		if err := bk.Delete([]byte(oldname)); err != nil {
			return err
		}
		return bk.Put([]byte(newname), m.marshal())
	})
}

func (p *Package) Copy(from, to string) error {
	f, err := p.Read(from)
	if err != nil {
		return err
	}
	defer f.Close()
	return p.Write(to, f)
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

func (p *Package) ForEach(f func(Meta, io.Reader) error) error {
	return p.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(trunkBucket).Cursor()
		for k, v := c.First(); len(k) > 0; k, v = c.Next() {
			sk := string(k)
			if strings.HasPrefix(sk, "*:") {
				continue
			}
			r, err := p.Read(sk)
			if err != nil {
				return err
			}
			if err := f(unmarshalMeta(v), r); err != nil {
				r.Close()
				return err
			}
			r.Close()
		}
		return nil
	})
}
