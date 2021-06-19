package vfs

import (
	"strconv"

	bbolt "go.etcd.io/bbolt"
)

func (p *Package) Compact() error {
	eof := int64(0)
	err := p.db.Update(func(tx *bbolt.Tx) error {
		err := p.compactHoles(tx)
		if err != nil {
			return err
		}
		eof, err = p.calcTruncate(tx)
		return err
	})
	if err != nil {
		return err
	}
	return p.data.Truncate(eof)
}

func (p *Package) compactHoles(tx *bbolt.Tx) error {
	for s := BlockSize_1K; s < BlockSize_16M; s *= 2 {
		c := tx.Bucket([]byte("holes_" + strconv.Itoa(s))).Cursor()
		holes := []BlockPos{}
		for k, _ := c.First(); len(k) == 8; k, _ = c.Next() {
			holes = append(holes, unmarshalBlockPos(k))
		}
		if len(holes) < 2 {
			continue
		}
		for i := 0; i < len(holes)-1; {
			a, b := holes[i], holes[i+1]
			if a.Size() == b.Size() && a.End() == b.Start() {
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

func (p *Package) calcTruncate(tx *bbolt.Tx) (int64, error) {
	eof, err := p.data.Seek(0, 2)
	if err != nil {
		return 0, err
	}
	for cont := true; cont; {
		cont = false
		for s := BlockSize_1K; s <= BlockSize_16M; s *= 2 {
			c := tx.Bucket([]byte("holes_" + strconv.Itoa(s))).Cursor()
			for k, _ := c.Last(); len(k) == 8; k, _ = c.Prev() {
				h := unmarshalBlockPos(k)
				if h.End() == eof {
					eof -= h.Size()
					if err := h.deleteFromHole(tx); err != nil {
						return 0, err
					}
					cont = true
					continue
				}
				break
			}
		}
	}
	return eof, nil
}
