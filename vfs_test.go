package vfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func random(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(rand.Int())
	}
	return buf
}

func write(name string, buf []byte) {
	ioutil.WriteFile("test/"+name, buf, 0777)
}

func read(name string) (buf []byte) {
	buf, _ = ioutil.ReadFile("test/" + name)
	return
}

func TestConst(t *testing.T) {
	rand.Seed(time.Now().Unix())

	// bp := NewBlockPos(10, BlockSize_16M)
	// t.Log(bp.SplitToSize(BlockSize_1K * 4))
	run(t, 0)
	// run(t, 5)
}

func run(t *testing.T, v int) {
	testFlagSimulateDataWriteError = v
	os.RemoveAll("test")
	os.MkdirAll("test", 0777)

	p, _ := Open("test")
	defer p.Close()
	fmt.Println(p.Stat())
	// return
	// p.ForEach(func(k Meta, r io.Reader) error {
	// 	buf, _ := ioutil.ReadAll(r)
	// 	fmt.Println(k.Name, k.Positions, len(buf))
	// 	return nil
	// })
	// return

	m := map[string]int{}

	if true {
		for _, i := range rand.Perm(20) {
			var x []byte
			if rand.Intn(2) == 1 {
				x = random(rand.Intn(BlockSize_16M * 3))
			} else {
				x = random(rand.Intn(BlockSize_1K * 1024))
			}
			key := "zzz" + strconv.Itoa(i)
			if fmt.Sprint(p.Write(key, bytes.NewReader(x))) != "testable" {
				write(key, x)
				m[key] = 1
			}
		}

		for k := range m {
			delete(m, k)
			p.Delete(k)
			if rand.Intn(len(m)/4+1) == 0 {
				break
			}
		}

		for _, i := range rand.Perm(15) {
			i += 10
			var x []byte
			if rand.Intn(2) == 1 {
				x = random(rand.Intn(BlockSize_16M * 2))
			} else {
				x = random(rand.Intn(BlockSize_1K * 1024 * 3))
			}
			key := "zzz" + strconv.Itoa(i)
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
			p.ForEach(func(m Meta, r io.Reader) error {
				if m.Name == k {
					fmt.Println(m.Positions)
				}
				return nil
			})
			t.Fatal(k, len(buf1), len(buf2))
		}
	}
}
