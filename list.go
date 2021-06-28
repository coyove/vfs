package vfs

import (
	"io"
	"path/filepath"
	"strings"

	"go.etcd.io/bbolt"
)

func (p *Package) ForEach(toplevel string, f func(Meta, io.Reader) error) error {
	return p.forEachImpl(toplevel, true, func(m Meta, r io.Reader) error { return f(m, r) })
}

func (p *Package) ForEachMeta(toplevel string, f func(Meta) error) error {
	return p.forEachImpl(toplevel, false, func(m Meta, r io.Reader) error { return f(m) })
}

func (p *Package) forEachImpl(toplevel string, reader bool, f func(Meta, io.Reader) error) error {
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
			if reader {
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
			} else {
				if err := f(unmarshalMeta(v), nil); err != nil {
					if err == ErrAbort {
						return nil
					}
					return err
				}
			}
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
					idx := strings.Index(dir, name)       // 1st: /root/www/xxx/yyyNAMEyyy/zzz
					idx2 := strings.Index(dir[idx:], "/") // 2nd: NAMEyyy/zzz      ^
					if idx2 == -1 {                       //             ^
						idx = len(dir)
					} else {
						idx += idx2
					}
					dir = strings.TrimSuffix(dir[:idx], "/") // 3rd: /root/www/xxx/yyyNAMEyyy
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
