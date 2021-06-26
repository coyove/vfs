package vfs

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func write(name string, buf []byte) {
	ioutil.WriteFile("test"+name, buf, 0777)
}

func read(name string) (buf []byte) {
	buf, _ = ioutil.ReadFile("test" + name)
	return
}

func hash(name string) uint32 {
	buf, _ := ioutil.ReadFile("test" + name)
	return crc32.ChecksumIEEE(buf)
}

func TestConst(t *testing.T) {
	run(t, 0)
	run(t, 5)
}

func run(t *testing.T, v int) {
	testFlagSimulateDataWriteError = v
	os.RemoveAll("test")
	os.MkdirAll("test", 0777)

	p, err := Open("test")
	if err != nil {
		panic(err)
	}

	defer p.Close()
	fmt.Println(p.Stat())

	// iif, _ := os.Open("test.dat")
	// start := time.Now()
	// p.Write("/big", iif)
	// fmt.Println(time.Since(start).Seconds())
	// return

	m := map[string]int{}
	if key := "/zero"; fmt.Sprint(p.WriteAll(key, nil)) != "testable" {
		write(key, nil)
		m[key] = 1
	}

	appends := map[string][]byte{}
	for _, i := range rand.Perm(20) {
		var x []byte
		if rand.Intn(2) == 1 {
			x = random(rand.Intn(16 * 1024 * 1024 * 3))
		} else {
			x = random(rand.Intn(1024 * 1024))
		}
		key := "/zzz" + strconv.Itoa(i)
		full := x
		if len(x) > BlockSize && rand.Intn(2) == 0 {
			appends[key] = x[BlockSize:]
			x = x[:BlockSize]
		}
		if fmt.Sprint(p.Write(key, bytes.NewReader(x))) != "testable" {
			write(key, full)
			m[key] = 1
		} else {
			delete(appends, key)
		}
	}

	oldFlag := testFlagSimulateDataWriteError
	for key, x := range appends {
		fmt.Println("append", key)
	AGAIN:
		if err := p.Append(key, bytes.NewReader(x)); err != nil {
			if fmt.Sprint(err) != "testable" {
				panic(err)
			}
			fmt.Println("append", key, "retry")
			testFlagSimulateDataWriteError *= 2
			goto AGAIN
		}
	}
	testFlagSimulateDataWriteError = oldFlag

	if true {
		for k := range m {
			if rand.Intn(2) == 0 {
				delete(m, k)
				p.Delete(k)
			} else {
				if fmt.Sprint(p.Copy(k, k+"copy")) != "testable" {
					m[k+"copy"] = 1
					write(k+"copy", read(k))
				}
			}
			if rand.Intn(len(m)/4+1) == 0 {
				break
			}
		}

		for _, i := range rand.Perm(15) {
			i += 10
			var x []byte
			if rand.Intn(2) == 1 {
				x = random(rand.Intn(16 * 1024 * 1024))
			} else {
				x = random(rand.Intn(4 * 1024 * 1024))
			}
			key := "/zzz" + strconv.Itoa(i)
			if fmt.Sprint(p.Write(key, bytes.NewReader(x))) != "testable" {
				write(key, x)
				m[key] = 1
			}
		}
	}

	for k := range m {
		buf1, _ := p.ReadAll(k)
		buf2 := read(k)
		if !bytes.Equal(buf1, buf2) {
			write("a", buf1)
			write("b", buf2)
			t.Fatal(k, len(buf1), len(buf2))
		}

		m, _ := p.Info(k)
		if h := hash(k); m.Crc32 != h {
			t.Fatal(k, m.Crc32, h)
		}

	}

	for k := range m {
		func() {
			f1, _ := p.Open(k)
			defer f1.Close()
			f2, _ := os.Open("test/" + k)
			defer f2.Close()

			sz := f1.size
			if sz == 0 {
				return
			}

			for i := 0; i < 10; i++ {
				off := int64(rand.Intn(int(sz)))
				f1.Seek(off, 0)
				f2.Seek(off, 0)
				sz := int64(rand.Intn(int(f1.size - off)))
				x1, x2 := make([]byte, sz), make([]byte, sz)
				n1, _ := io.ReadFull(f1, x1)
				n2, _ := io.ReadFull(f2, x2)
				x1 = x1[:n1]
				x2 = x2[:n2]

				if !bytes.Equal(x1, x2) {
					t.Fatal(k, len(x1), len(x2))
				}
			}
		}()
	}
}

func TestDir(t *testing.T) {
	p, _ := Open("test")
	p.WriteAll("/a.txt", []byte("1"))
	p.WriteAll("/c.txt", []byte("1"))
	p.WriteAll("/b/a.txt", []byte("1"))
	p.WriteAll("/b/d.txt", []byte("1"))
	p.WriteAll("/b/e/1.txt", []byte("12"))
	p.WriteAll("/b/f.txt", []byte("100"))
	fmt.Println(p.WriteAll("/b", []byte("100")))
	fmt.Println(p.List("/"))
	fmt.Println(p.List("/b"))
	fmt.Println(p.Info("/b"))
	fmt.Println(p.Info("/b/e"))
}

func TestWalk(t *testing.T) {
	p, _ := Open("testtmp")
	hh := map[string]uint32{}
	total := 0
	start := time.Now()
	testFlagSimulateDataWriteError = 0
	filepath.Walk(os.TempDir(), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}

		f, _ := os.Open(path)
		defer f.Close()

		h := crc32.NewIEEE()
		if err := p.Write(path, io.TeeReader(f, h)); err != nil {
			t.Fatal(err, path)
		}
		hh[path] = h.Sum32()
		total += int(info.Size())
		// if total > 500*1024*1024 {
		// 	return ErrAbort
		// }
		return nil
	})

	fmt.Println("verify", float64(total)/time.Since(start).Seconds()/1024/1024)

	p.ForEach("/var", func(m Meta, r io.Reader) error {
		if _, ok := hh[m.Name]; !ok {
			return nil
		}
		buf, _ := ioutil.ReadAll(r)
		x := crc32.ChecksumIEEE(buf)
		if x != hh[m.Name] {
			t.Fatal(m.Name, x, hh[m.Name])
		}
		return nil
	})
}

func TestList(t *testing.T) {
	p, _ := Open("testtmp")
	fmt.Println(listrec(p, "/"))
	fmt.Println(p.Info("/Users"))
	fmt.Println(p.Info("/var"))
}

func listrec(p *Package, root string) int {
	m, _ := p.List(root)
	count := 0
	for _, m := range m {
		if m.IsDir {
			count += listrec(p, m.Name)
		} else {
			fmt.Println(m.Name)
			count++
		}
	}
	return count
}
