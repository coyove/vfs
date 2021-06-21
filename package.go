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
	dbpath string
	db     *bbolt.DB
	data   *os.File
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
		db:     db,
		dbpath: path,
		data:   f,
	}
	return p, p.Compact()
}

func (p *Package) Close() error {
	if err1, err2 := p.db.Close(), p.data.Close(); err1 != nil || err2 != nil {
		return fmt.Errorf("close package: %v or %v", err1, err2)
	}
	return nil
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

func (p *Package) Info(key string) (m Meta, err error) {
	if !checkName(key) {
		return m, ErrInvalidName
	}
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
	m, err := p.Info(key)
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
		m, err := p.Info(key)
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
			if err := p.incTotalSize(tx, -old.Size, -1); err != nil {
				return err
			}
			defer func() {
				if E == nil {
					E = old.Positions.Free(tx)
				}
			}()
			// Write data to new blocks, then recycle old blocks in the above defer-call
		}

		buf, clean := bigBuffer()
		small := bytes.Buffer{}
		h := sha1.New()
		beforeEOF := bytesToInt64(bk.Get(dataSizeKey))

		defer func() {
			if E != nil {
				// If encountered error, data file may be appended with unwanted bytes already
				bk.Put(dataSizeKey, int64ToBytes(beforeEOF))
			}
			clean()
		}()

		for {
			n, err := value.Read(buf)
			if n > 0 {
				m.Size += int64(n)
				if small.Len() < SmallBlockSize {
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

		if m.Size < SmallBlockSize {
			// Store small data outside data file to reduce fragments
			if err := m.Positions.Free(tx); err != nil {
				return err
			}
			m.SmallData = small.Bytes()
			m.Positions = nil
		}

		if err := p.incTotalSize(tx, m.Size, 1); err != nil {
			return err
		}
		// fmt.Println(m.Positions)
		copy(m.Sha1[:], h.Sum(nil))
		return bk.Put(keybuf, m.marshal())
	})
}

func (p *Package) Delete(key string) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		m, err := p.Info(key)
		if err != nil {
			return err
		}
		if err := m.Positions.Free(tx); err != nil {
			return err
		}
		if err := p.incTotalSize(tx, -m.Size, -1); err != nil {
			return err
		}
		return tx.Bucket(trunkBucket).Delete([]byte(key))
	})
}

func (p *Package) Rename(oldname, newname string) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		old, err := p.Info(oldname)
		if err != nil {
			return err
		}
		if _, err := p.Info(newname); err != ErrNotFound {
			return fmt.Errorf("rename: new name error: %v", err)
		}

		old.Name = newname
		bk := tx.Bucket(trunkBucket)
		if err := bk.Delete([]byte(oldname)); err != nil {
			return err
		}
		return bk.Put([]byte(newname), old.marshal())
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

func (p *Package) incTotalSize(tx *bbolt.Tx, sz, cnt int64) error {
	bk := tx.Bucket(trunkBucket)
	if err := bk.Put(totalSizeKey, int64ToBytes(bytesToInt64(bk.Get(totalSizeKey))+sz)); err != nil {
		return err
	}
	if err := bk.Put(totalCountKey, int64ToBytes(bytesToInt64(bk.Get(totalCountKey))+cnt)); err != nil {
		return err
	}
	return nil
}

// Size returns 2 values: used bytes in database, and actual bytes on disk
func (p *Package) Size() (sz, diskSize int64) {
	diskSize, _ = p.data.Seek(0, 2)
	if fi, _ := os.Stat(p.dbpath); fi != nil {
		diskSize += fi.Size()
	}
	p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		sz = bytesToInt64(bk.Get(totalSizeKey))
		return nil
	})
	return
}

func (p *Package) Count() (c int64) {
	p.db.View(func(tx *bbolt.Tx) error {
		c = bytesToInt64(tx.Bucket(trunkBucket).Get(totalCountKey))
		return nil
	})
	return
}

func (p *Package) FreeBlocks() (frees int64) {
	p.db.View(func(tx *bbolt.Tx) error {
		frees = int64(tx.Bucket(freeBucket).Stats().KeyN)
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
				if err == ErrAbort {
					return nil
				}
				return err
			}
			r.Close()
		}
		return nil
	})
}

func (p *Package) Search(name string, max int) (names []Meta, err error) {
	err = p.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(trunkBucket).Cursor()
		for k, v := c.First(); len(k) > 0 && len(names) < max; k, v = c.Next() {
			sk := string(k)
			if strings.HasPrefix(sk, "*:") {
				continue
			}
			if strings.Contains(sk, name) {
				names = append(names, unmarshalMeta(v))
			}
		}
		return nil
	})
	return
}

// 				d := Dir{
// 					Name:       path + suffix[:idx+1],
// 					Size:       gjson.GetBytes(v, "sz").Int(),
// 					Count:      1,
// 					CreateTime: gjson.GetBytes(v, "ct").Int(),
// 					ModTime:    gjson.GetBytes(v, "mt").Int(),
// 				}
// 				for {
// 					k, v = c.Next()
// 					if strings.HasPrefix(*(*string)(unsafe.Pointer(&k)), d.Name) {
// 						d.Size += gjson.GetBytes(v, "sz").Int()
// 						d.Count++
// 						ct := gjson.GetBytes(v, "ct").Int()
// 						mt := gjson.GetBytes(v, "mt").Int()
// 						if ct < d.CreateTime {
// 							d.CreateTime = ct
// 						}
// 						if mt > d.ModTime {
// 							d.ModTime = mt
// 						}
// 						continue
// 					}
// 					c.Prev()
// 					break
// 				}
// 				names = append(names, d)

func (p *Package) List(path string) (names []interface{}, err error) {
	path = strings.TrimSuffix(path, "/") + "/"
	err = p.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(trunkBucket).Cursor()
		for k, v := c.Seek([]byte(path)); len(k) > 0; {
			sk := string(k)
			if strings.HasPrefix(sk, "*:") {
				k, v = c.Next()
				continue
			}
			if !strings.HasPrefix(sk, path) {
				break
			}
			suffix := sk[len(path):]
			if idx := strings.Index(suffix, "/"); idx > -1 {
				d := Dir{Name: path + suffix[:idx+1]}
				names = append(names, d)
				k, v = c.Seek([]byte(d.Name + "\xff"))
			} else {
				names = append(names, unmarshalMeta(v))
				k, v = c.Next()
			}
		}
		return nil
	})
	return
}
