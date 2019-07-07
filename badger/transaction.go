package badger

import (
	badgerdb "github.com/dgraph-io/badger"
	"github.com/osiloke/gostore"
)

type BadgerTransaction struct {
	txn *badgerdb.Txn
}

func (t *BadgerTransaction) Commit() error {
	return t.txn.Commit()
}

func (t *BadgerTransaction) Discard() {
	t.txn.Discard()
}

func (t *BadgerTransaction) Set(key []byte, data []byte) error {
	return gostore.ErrNotImplemented
}
func (t *BadgerTransaction) Get(key []byte) ([]byte, error) {
	return nil, gostore.ErrNotImplemented

}
func (t *BadgerTransaction) Delete(key []byte) error {

	return gostore.ErrNotImplemented
}
