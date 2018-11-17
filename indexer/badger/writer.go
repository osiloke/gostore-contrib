package badger

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
)

type Writer struct {
	s *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	return &Batch{
		store: w.s,
		merge: store.NewEmulatedMerge(w.s.mo),
		Txn:   w.s.db.NewTransaction(true),
	}
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batch, ok := b.(*Batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	// first process merges
	for k, mergeOps := range batch.merge.Merges {
		kb := []byte(k)
		item, err := batch.Txn.Get(kb)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		var v []byte
		if err != badger.ErrKeyNotFound {
			err = item.Value(func(val []byte) error {
				v = append([]byte{}, val...)
				return nil
			})

			if err != nil {
				return err
			}
		}
		mergedVal, fullMergeOk := w.s.mo.FullMerge(kb, v, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		batch.Txn.Set(kb, mergedVal)
	}

	return batch.Txn.Commit()
}

func (w *Writer) Close() error {
	w.s = nil
	return nil
}
