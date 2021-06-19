package vfs

import (
	"bytes"
	"fmt"
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
	os.RemoveAll("test")
	os.MkdirAll("test", 0777)

	bp := NewBlockPos(10, BlockSize_16M)
	// t.Log(bp.SplitToSize(BlockSize_1K * 4))

	p, _ := Open("test")
	fmt.Println(p.Stat())

	m := map[string]int{}
	for _, i := range rand.Perm(20) {
		var x []byte
		if rand.Intn(2) == 1 {
			x = random(rand.Intn(BlockSize_16M * 3))
		} else {
			x = random(rand.Intn(BlockSize_1K * 1024))
		}
		key := "zzz" + strconv.Itoa(i)
		p.Write(key, bytes.NewReader(x))
		write(key, x)
		m[key] = 1
	}

	if true {
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
				x = random(rand.Intn(BlockSize_16M))
			} else {
				x = random(rand.Intn(BlockSize_1K * 1024 * 3))
			}
			key := "zzz" + strconv.Itoa(i)
			p.Write(key, bytes.NewReader(x))
			write(key, x)
			m[key] = 1
		}
	}

	for k := range m {
		buf1, _ := p.ReadAll(k)
		buf2 := read(k)
		if !bytes.Equal(buf1, buf2) {
			write("a", buf1)
			write("b", buf2)
			t.Fatal(len(buf1), len(buf2))
		}
	}
}
