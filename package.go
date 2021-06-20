package vfs

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
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
	db, err := bbolt.Open(path+".index", 0777, nil)
	if err != nil {
		return nil, err
	}

	dataFileHash := ""
	dataFileMinSize := int64(0)
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(trashBucket); err != nil {
			return err
		}

		trunk, err := tx.CreateBucketIfNotExists(trunkBucket)
		if err != nil {
			return err
		}

		dataFileMinSize = bytesToInt64(trunk.Get(dataSizeKey))

		h := trunk.Get(dataFileKey)
		if len(h) != 8 {
			h = random(8)
		}
		dataFileHash = hex.EncodeToString(h)
		if err := trunk.Put(dataFileKey, h); err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(freeBucket)
		return err
	}); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path+"."+dataFileHash+".data", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}
	if eof, err := f.Seek(0, 2); err != nil {
		return nil, err
	} else if eof < dataFileMinSize {
		return nil, fmt.Errorf("corrupted data file size: %v, require at least %v", eof, dataFileMinSize)
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

func (p *Package) writeData(buf []byte, off int64, padSize bool) error {
	if _, err := p.data.Seek(off, 0); err != nil {
		return err
	}

	if testFlagSimulateDataWriteError > 0 && rand.Intn(testFlagSimulateDataWriteError) == 0 {
		x := buf[:rand.Intn(len(buf))]
		fmt.Println("test flag: simulate data write error, off=", off, "size=", len(buf), "write=", len(x))
		p.data.Write(x)
		return fmt.Errorf("testable")
	}

	n, err := p.data.Write(buf)
	if err != nil || n != len(buf) {
		return fmt.Errorf("write data: %v, written: %v", err, n)
	}

	if padSize && n < BlockSize {
		paddings := make([]byte, BlockSize-n)
		n, err = p.data.Write(paddings)
		if err != nil || n != len(paddings) {
			return fmt.Errorf("write paddings: %v, written: %v", err, n)
		}
	}
	return nil
}

func (p *Package) putData(tx *bbolt.Tx, buf []byte) (int64, error) {
	assert(len(buf) <= BlockSize)

	bk := tx.Bucket(freeBucket)
	if k, _ := bk.Cursor().First(); len(k) == 4 {
		boff := bytesToUint32(k)
		off := int64(boff) * BlockSize
		if err := allocBlock(tx, boff); err != nil {
			return 0, err
		}
		return off, p.writeData(buf, off, false)
	}
	// No free blocks, create one at the end of data file
	trunk := tx.Bucket(trunkBucket)
	eof := bytesToInt64(trunk.Get(dataSizeKey))
	if err := p.writeData(buf, eof, true); err != nil {
		return 0, err
	}
	return eof, trunk.Put(dataSizeKey, int64ToBytes(eof+BlockSize))
}

func (p *Package) ReadAll(key string) ([]byte, error) {
	r, err := p.Open(key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (p *Package) Meta(key string) (m Meta, err error) {
	keybuf := []byte(key)
	err = p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		if len(metabuf) == 0 {
			return ErrNotFound
		}
		m = unmarshalMeta(metabuf)
		return nil
	})
	return m, err
}

func (p *Package) Open(key string) (*File, error) {
	m, err := p.Meta(key)
	if err != nil {
		return nil, err
	}

	if len(m.SmallData) == int(m.Size) {
		return &File{size: int64(len(m.SmallData)), small: m.SmallData}, nil
	}

	f, err := os.OpenFile(p.data.Name(), os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	r := &File{f: f, size: m.Size, offsets: make([]int64, 0, len(m.Positions)/2)}
	m.Positions.ForEach(func(v uint32) error {
		r.offsets = append(r.offsets, int64(v)*BlockSize)
		return nil
	})
	return r, nil
}

func (p *Package) UpdateTags(key string, f func(map[string]string) error) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		m, err := p.Meta(key)
		if err != nil {
			return err
		}
		if m.Tags == nil {
			m.Tags = map[string]string{}
		}
		if err := f(m.Tags); err != nil {
			return err
		}
		return tx.Bucket(trunkBucket).Put([]byte(key), m.marshal())
	})
}

func (p *Package) WriteAll(key string, value []byte, kvs ...string) error {
	return p.Write(key, bytes.NewReader(value), kvs...)
}

func (p *Package) Write(key string, value io.Reader, kvs ...string) error {
	if !checkName(key) {
		return ErrInvalidName
	}
	if len(kvs)%2 == 1 {
		return fmt.Errorf("invalid key value pairs")
	}
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) (E error) {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		m := Meta{
			Name:       key,
			CreateTime: time.Now().Unix(),
			ModTime:    time.Now().Unix(),
			Tags:       kvsToMap(kvs...),
		}

		if len(metabuf) > 0 {
			// Overwrite existing data
			old := unmarshalMeta(metabuf)
			m.CreateTime = old.CreateTime
			if err := p.incTotalSize(tx, trunkBucket, -old.Size, -1); err != nil {
				return err
			}
			defer func() {
				if E == nil {
					E = old.Positions.Free(tx)
				}
			}()
			// Write data to new blocks, then recycle old blocks in the above defer-call
		}

		if max := bytesToInt64(bk.Get(maxSizeKey)); max > 0 && bytesToInt64(bk.Get(totalSizeKey)) > max {
			return fmt.Errorf("package max size reached: %v", max)
		}

		buf, clean := bigBuffer()
		small := bytes.Buffer{}
		h := sha1.New()
		defer clean()

		for {
			n, err := value.Read(buf)
			if n > 0 {
				m.Size += int64(n)
				if small.Len() < BlockSize {
					small.Write(buf[:n])
				}
				h.Write(buf[:n])
				bp, err := p.putData(tx, buf[:n])
				if err != nil {
					return err
				}
				// fmt.Println("write", bp)
				m.Positions.Append(uint32(bp / BlockSize))
			}
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}

		if m.Size < BlockSize/2 {
			// Small data
			if err := m.Positions.Free(tx); err != nil {
				return err
			}
			m.SmallData = small.Bytes()
			m.Positions = nil
		}

		if err := p.incTotalSize(tx, trunkBucket, m.Size, 1); err != nil {
			return err
		}
		// fmt.Println(m.Positions)
		copy(m.Sha1[:], h.Sum(nil))
		return bk.Put(keybuf, m.marshal())
	})
}

func (p *Package) Restore(key string) error {
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trashBucket)
		metabuf := bk.Get(keybuf)
		if len(metabuf) == 0 {
			return ErrNotFound
		}
		m := unmarshalMeta(metabuf)
		if err := p.incTotalSize(tx, trunkBucket, m.Size, 1); err != nil {
			return err
		}
		if err := p.incTotalSize(tx, trashBucket, -m.Size, -1); err != nil {
			return err
		}
		if err := bk.Delete(keybuf); err != nil {
			return err
		}
		return tx.Bucket(trunkBucket).Put(keybuf, metabuf)
	})
}

func (p *Package) Delete(key string, trash bool) error {
	keybuf := []byte(key)
	return p.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		metabuf := bk.Get(keybuf)
		if len(metabuf) == 0 {
			return ErrNotFound
		}
		m := unmarshalMeta(metabuf)
		if trash {
			if err := tx.Bucket(trashBucket).Put(keybuf, metabuf); err != nil {
				return err
			}
			if err := p.incTotalSize(tx, trashBucket, m.Size, 1); err != nil {
				return err
			}
		} else {
			if err := m.Positions.Free(tx); err != nil {
				return err
			}
		}
		if err := p.incTotalSize(tx, trunkBucket, -m.Size, -1); err != nil {
			return err
		}
		return bk.Delete(keybuf)
	})
}

func (p *Package) Rename(oldname, newname string) error {
	if !checkName(newname) {
		return ErrInvalidName
	}
	return p.db.Update(func(tx *bbolt.Tx) error {
		m, err := p.Meta(oldname)
		if err != nil {
			return err
		}
		if m.Name == newname {
			return fmt.Errorf("rename: new name already occupied")
		}

		m.Name = newname
		bk := tx.Bucket(trunkBucket)
		if err := bk.Delete([]byte(oldname)); err != nil {
			return err
		}
		return bk.Put([]byte(newname), m.marshal())
	})
}

func (p *Package) Copy(from, to string) error {
	f, err := p.Open(from)
	if err != nil {
		return err
	}
	defer f.Close()
	return p.Write(to, f)
}

func (p *Package) incTotalSize(tx *bbolt.Tx, bkName []byte, sz, cnt int64) error {
	bk := tx.Bucket(bkName)
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

func (p *Package) Stat() (totalSize, totalCount, freeBlocks int64) {
	p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		totalSize = bytesToInt64(bk.Get(totalSizeKey))
		totalCount = bytesToInt64(bk.Get(totalCountKey))
		freeBlocks = int64(tx.Bucket(freeBucket).Stats().KeyN)
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
			r, err := p.Open(sk)
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

func (p *Package) SetMaxSize(v int64) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(trunkBucket).Put(maxSizeKey, int64ToBytes(v))
	})
}

func (p *Package) ListAll(prefix string) (names []Meta, err error) {
	return p.listAll(prefix, trunkBucket)
}

func (p *Package) ListAllTrash(prefix string) (names []Meta, err error) {
	return p.listAll(prefix, trashBucket)
}

func (p *Package) listAll(prefix string, bkName []byte) (names []Meta, err error) {
	err = p.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bkName).Cursor()
		for k, v := c.First(); len(k) > 0; k, v = c.Next() {
			sk := string(k)
			if !strings.HasPrefix(sk, "*:") && strings.HasPrefix(sk, prefix) {
				names = append(names, unmarshalMeta(v))
			}
		}
		return nil
	})
	return
}