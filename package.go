package vfs

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	bbolt "go.etcd.io/bbolt"
)

var testFlagSimulateDataWriteError = 0

type Package struct {
	dbpath string
	db     *bbolt.DB
	data   *os.File
	buffer []byte // can only be used in a locked environment
}

func Open(path string) (*Package, error) {
	path = strings.TrimSuffix(path, ".index")
	db, err := bbolt.Open(path+".index", 0777, nil)
	if err != nil {
		return nil, err
	}

	dataFileHash := ""
	if err := db.Update(func(tx *bbolt.Tx) error {
		trunk, err := tx.CreateBucketIfNotExists(trunkBucket)
		if err != nil {
			return err
		}
		h := trunk.Get(dataFileKey)
		if len(h) != 8 {
			h = random(8)
		}
		dataFileHash = hex.EncodeToString(h)
		return trunk.Put(dataFileKey, h)
	}); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path+"."+dataFileHash+".data", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	p := &Package{
		db:     db,
		dbpath: path,
		data:   f,
		buffer: make([]byte, BlockSize),
	}
	return p, nil
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

func (p *Package) putData(tx *bbolt.Tx, buf []byte, c *FreeBitmapCursor) (int64, error) {
	assert(len(buf) <= BlockSize)

	boff, newBlock := c.Next()
	off := int64(boff) * BlockSize
	return off, p.writeData(buf, off, newBlock)
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
			count := bytesToInt64(bk.Get([]byte(string(totalCountKey) + key)))
			if count > 0 {
				// Is a top level directory
				size := bytesToInt64(bk.Get([]byte(string(totalSizeKey) + key)))
				m = Meta{Name: key + "/", IsDir: true, Size: size, Count: count}
				return nil
			}
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
	if m.IsDir {
		return nil, fmt.Errorf("open: directory")
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
		if m.IsDir {
			return fmt.Errorf("update: directory")
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
			if err := p.incTotalSize(tx, key, -old.Size, -1); err != nil {
				return err
			}
			defer func() {
				if E == nil {
					E = old.Positions.Free(tx)
				}
			}()
			// Write data to new blocks, then recycle old blocks in the above defer-call
		} else {
			// Check name collision between file and dir, e.g.: "/a/" and "/a"
			dirbuf := []byte(key + "/")
			k, _ := bk.Cursor().Seek(dirbuf)
			if bytes.HasPrefix(k, dirbuf) {
				return fmt.Errorf("dir name collision")
			}
		}

		small := bytes.Buffer{}
		h := crc32.NewIEEE()
		beforeEOF, err := p.data.Seek(0, 2)
		if err != nil {
			return err
		}

		defer func() {
			if E != nil {
				// If encountered error, data file may be appended with unwanted bytes already
				p.data.Truncate(beforeEOF)
			}
		}()

		freeMap := FreeBitmap(append([]byte{}, bk.Get(freeKey)...))
		c := &FreeBitmapCursor{src: freeMap}
		for {
			n, err := value.Read(p.buffer)
			if n > 0 {
				m.Size += int64(n)
				if small.Len() < SmallBlockSize {
					small.Write(p.buffer[:n])
				}
				h.Write(p.buffer[:n])
				bp, err := p.putData(tx, p.buffer[:n], c)
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

		if err := p.incTotalSize(tx, key, m.Size, 1); err != nil {
			return err
		}
		if err := bk.Put(freeKey, c.src); err != nil {
			return err
		}

		// fmt.Println(m.Name, len(m.Positions))
		m.Crc32 = h.Sum32()
		return bk.Put(keybuf, m.marshal())
	})
}

// func (p *Package) Append(key string, value io.Reader) error {
// 	keybuf := []byte(key)
// 	return p.db.Update(func(tx *bbolt.Tx) (E error) {
// 		bk := tx.Bucket(trunkBucket)
// 		m, err := p.Info(key)
// 		if err != nil {
// 			return err
// 		}
// 		if m.IsDir {
// 			return fmt.Errorf("append: directory")
// 		}
//
// 		if len(metabuf) > 0 {
// 			// Overwrite existing data
// 			old := unmarshalMeta(metabuf)
// 			m.CreateTime = old.CreateTime
// 			if err := p.incTotalSize(tx, key, -old.Size, -1); err != nil {
// 				return err
// 			}
// 			defer func() {
// 				if E == nil {
// 					E = old.Positions.Free(tx)
// 				}
// 			}()
// 			// Write data to new blocks, then recycle old blocks in the above defer-call
// 		} else {
// 			// Check name collision between file and dir, e.g.: "/a/" and "/a"
// 			dirbuf := []byte(key + "/")
// 			k, _ := bk.Cursor().Seek(dirbuf)
// 			if bytes.HasPrefix(k, dirbuf) {
// 				return fmt.Errorf("dir name collision")
// 			}
// 		}
//
// 		small := bytes.Buffer{}
// 		h := crc32.New()
// 		beforeEOF, err := p.data.Seek(0, 2)
// 		if err != nil {
// 			return err
// 		}
//
// 		defer func() {
// 			if E != nil {
// 				// If encountered error, data file may be appended with unwanted bytes already
// 				p.data.Truncate(beforeEOF)
// 			}
// 		}()
//
// 		freeMap := FreeBitmap(append([]byte{}, bk.Get(freeKey)...))
// 		c := &FreeBitmapCursor{src: freeMap}
// 		for {
// 			n, err := value.Read(p.buffer)
// 			if n > 0 {
// 				m.Size += int64(n)
// 				if small.Len() < SmallBlockSize {
// 					small.Write(p.buffer[:n])
// 				}
// 				h.Write(p.buffer[:n])
// 				bp, err := p.putData(tx, p.buffer[:n], c)
// 				if err != nil {
// 					return err
// 				}
// 				// fmt.Println("write", bp)
// 				m.Positions.Append(uint32(bp / BlockSize))
// 			}
// 			if n == 0 || err == io.EOF {
// 				break
// 			}
// 			if err != nil {
// 				return err
// 			}
// 		}
//
// 		if m.Size < SmallBlockSize {
// 			// Store small data outside data file to reduce fragments
// 			if err := m.Positions.Free(tx); err != nil {
// 				return err
// 			}
// 			m.SmallData = small.Bytes()
// 			m.Positions = nil
// 		}
//
// 		if err := p.incTotalSize(tx, key, m.Size, 1); err != nil {
// 			return err
// 		}
// 		if err := bk.Put(freeKey, c.src); err != nil {
// 			return err
// 		}
//
// 		// fmt.Println(m.Name, len(m.Positions))
// 		copy(m.Sha1[:], h.Sum(nil))
// 		return bk.Put(keybuf, m.marshal())
// 	})
// }

func (p *Package) Delete(key string) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		m, err := p.Info(key)
		if err != nil {
			return err
		}
		if m.IsDir {
			return fmt.Errorf("delete: directory")
		}
		if err := m.Positions.Free(tx); err != nil {
			return err
		}
		if err := p.incTotalSize(tx, key, -m.Size, -1); err != nil {
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
		if old.IsDir {
			return fmt.Errorf("rename: directory")
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

func (p *Package) incTotalSize(tx *bbolt.Tx, name string, sz, cnt int64) error {
	bk := tx.Bucket(trunkBucket)

	idx := strings.Index(name[1:], "/")
	if idx > -1 {
		first := name[:1+idx]
		key := []byte(string(totalSizeKey) + first)
		if err := bk.Put(key, int64ToBytes(bytesToInt64(bk.Get(key))+sz)); err != nil {
			return err
		}
		key = []byte(string(totalCountKey) + first)
		if err := bk.Put(key, int64ToBytes(bytesToInt64(bk.Get(key))+cnt)); err != nil {
			return err
		}
	}

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

func (p *Package) Count() (totalFiles, totalBlocks int64) {
	p.db.View(func(tx *bbolt.Tx) error {
		totalFiles = bytesToInt64(tx.Bucket(trunkBucket).Get(totalCountKey))
		totalBlocks = int64(len(tx.Bucket(trunkBucket).Get(freeKey)) * 8)
		return nil
	})
	return
}

func (p *Package) ForEach(toplevel string, f func(Meta, io.Reader) error) error {
	toplevel = strings.TrimSuffix(toplevel, "/") + "/"
	return p.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(trunkBucket).Cursor()
		k, v := c.First()
		if toplevel != "/" {
			k, v = c.Seek([]byte(toplevel))
		}
		for ; len(k) > 0; k, v = c.Next() {
			sk := string(k)
			if strings.HasPrefix(sk, "*:") {
				continue
			}
			if !strings.HasPrefix(sk, toplevel) {
				break
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

func (p *Package) Search(toplevel, name string, max int) (names []Meta, err error) {
	toplevel = strings.TrimSuffix(toplevel, "/") + "/"
	dedup := map[string]bool{}
	err = p.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(trunkBucket).Cursor()
		for k, v := c.Seek([]byte(toplevel)); len(k) > 0 && len(names) < max; k, v = c.Next() {
			sk := string(k)
			if strings.HasPrefix(sk, "*:") {
				continue
			}
			if !strings.HasPrefix(sk, toplevel) {
				break
			}
			if strings.Contains(sk, name) {
				dir := filepath.Dir(sk)
				fn := filepath.Base(sk)
				if strings.Contains(fn, name) {
					names = append(names, unmarshalMeta(v))
				} else if strings.Contains(dir, name) {
					idx := strings.Index(dir, name)       // 1st: /xxx/yyyNAMEyyy/zzz
					idx2 := strings.Index(dir[idx:], "/") // 2nd: NAMEyyy/zzz
					if idx2 == -1 {
						idx = len(dir)
					} else {
						idx += idx2
					}
					dir = strings.TrimSuffix(dir[:idx], "/") // 3rd: /xxx/yyyNAMEyyy/
					if dedup[dir] {
						continue
					}
					dedup[dir] = true
					names = append(names, Meta{Name: dir + "/", IsDir: true})
				}
			}
		}
		return nil
	})
	return
}

func (p *Package) List(path string) (names []Meta, err error) {
	path = strings.TrimSuffix(path, "/") + "/"
	err = p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		c := bk.Cursor()
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
				d := Meta{Name: path + suffix[:idx+1], IsDir: true}
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
