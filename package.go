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
	"strings"
	"time"
	"unsafe"

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
		dbpath: path + ".index",
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

		m.Crc32, err = p.ioCopy(tx, &m, value, func(data []byte) {
			if small.Len() < SmallBlockSize {
				small.Write(data)
			}
		})
		if err != nil {
			return err
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

		// fmt.Println(m.Name, len(m.Positions))
		return bk.Put(keybuf, m.marshal())
	})
}

func (p *Package) ioCopy(tx *bbolt.Tx, m *Meta, src io.Reader, onRead func([]byte)) (uint32, error) {
	h := crc32.NewIEEE()
	*(*uint32)((*(*[2]unsafe.Pointer)(unsafe.Pointer(&h)))[1]) = m.Crc32
	freeMap := FreeBitmap(append([]byte{}, tx.Bucket(trunkBucket).Get(freeKey)...))
	c := &FreeBitmapCursor{src: freeMap}
	for {
		n, err := src.Read(p.buffer)
		if n > 0 {
			m.Size += int64(n)
			h.Write(p.buffer[:n])
			onRead(p.buffer[:n])
			bp, err := p.putData(tx, p.buffer[:n], c)
			if err != nil {
				return 0, err
			}
			m.Positions.Append(uint32(bp / BlockSize))
		}
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Bucket(trunkBucket).Put(freeKey, c.src); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func (p *Package) Append(key string, value io.Reader) error {
	return p.db.Update(func(tx *bbolt.Tx) (E error) {
		bk := tx.Bucket(trunkBucket)
		m, err := p.Info(key)
		if err != nil {
			return err
		}
		if m.IsDir {
			return fmt.Errorf("append: directory")
		}
		if len(m.SmallData) == int(m.Size) {
			return fmt.Errorf("append: small data not supported")
		}
		if m.Size%BlockSize != 0 {
			return fmt.Errorf("append: data not aligned")
		}

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

		oldSize := m.Size
		m.Crc32, err = p.ioCopy(tx, &m, value, func([]byte) {})
		if err != nil {
			return err
		}

		if err := p.incTotalSize(tx, key, m.Size-oldSize, 0); err != nil {
			return err
		}
		return bk.Put([]byte(key), m.marshal())
	})
}

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

func (p *Package) Stat() (s struct {
	Size        int64 // Size of all stored files
	DiskSize    int64 // Actual disk size (index + data)
	Files       int64 // Total number of files
	AllocBlocks int64 // Total allocated blocks
	DataFile    string
	IndexFile   string
}) {
	s.DiskSize, _ = p.data.Seek(0, 2)
	if fi, _ := os.Stat(p.dbpath); fi != nil {
		s.DiskSize += fi.Size()
	}
	p.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(trunkBucket)
		s.Size = bytesToInt64(bk.Get(totalSizeKey))
		s.Files = bytesToInt64(bk.Get(totalCountKey))
		s.AllocBlocks = int64(len(bk.Get(freeKey)) * 8)
		return nil
	})
	s.DataFile = p.data.Name()
	s.IndexFile = p.dbpath
	return
}
