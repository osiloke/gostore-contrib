package kv

import (
	"github.com/cznic/kv"
)

func SeekBefore(db *kv.DB, key string, count int, skip int, bucket string) (objs [][][]byte, err error) {
	ixkey := []byte(bucket + key)
	c, _, err := db.Seek(ixkey)
	if err != nil {
		println(err.Error())
		return
	}
	var lim int = 0
	if skip > 0 {
		var skip_lim int = 1
		var target_count int = skip - 1
		for k, _, err := c.Prev(); k != nil; k, _, err = c.Prev() {
			if err != nil {
				return nil, err
			}
			if skip_lim >= target_count {
				break
			}
			skip_lim++
		}
	}

	for k, v, err := c.Prev(); err == nil; k, v, err = c.Prev() {
		println(k, v)
		objs = append(objs, [][]byte{k, v})
		lim++
		if lim == count {
			break
		}
	}
	return
}
