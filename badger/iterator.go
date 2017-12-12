package badger

import (
	"github.com/dgraph-io/badger"
)

type Iterator struct {
	iterator *badger.Iterator
}

func (i *Iterator) Seek(key []byte) {
	i.iterator.Seek(key)
}

func (i *Iterator) Next() {
	i.iterator.Next()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	ks := i.iterator.Item().Key()
	k := make([]byte, len(ks))
	copy(k, ks)

	return k
}

func (i *Iterator) Value() []byte {
	vs, _ := i.iterator.Item().Value()
	v := make([]byte, len(vs))
	copy(v, vs)

	return v
}

func (i *Iterator) Valid() bool {
	return i.iterator.Valid()
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
