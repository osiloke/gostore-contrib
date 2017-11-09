package copy

import (
	"fmt"
	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/badger"
	"github.com/osiloke/gostore-contrib/bolt"
	"github.com/osiloke/gostore-contrib/common"
	"github.com/osiloke/gostore-contrib/indexer"
	_ "github.com/osiloke/gostore-contrib/indexer/badger"
	"github.com/osiloke/gostore-contrib/log"
	"os"
	"os/signal"
	"path/filepath"
)

type KVStore interface {
	Close()
	All(count int, skip int, store string) (gostore.ObjectRows, error)
	AllCursor(store string) (gostore.ObjectRows, error)
	BatchInsertKV(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error)
	BatchInsertKVAndIndex(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error)
}

var logger = log.New("copy")

// CopyRows copies n rows from one db to another
func CopyRows(src, dst KVStore, count int, store string) (int, error) {
	// logger.Info("copy %s", store)
	dstRows := [][][]byte{}
	if grows, err := src.AllCursor(store); err == nil {
		rows := grows.(*common.CursorRows)
		defer rows.Close()
		var err2 error
		for i := 0; i < count; i++ {

			var kv [][]byte
			kv, err2 = rows.NextKV()
			if err2 != nil {
				break
			}
			dstRows = append(dstRows, kv)
			// logger.Debug("processing row ... %v", row)
		}
		total := len(dstRows)
		logger.Debug(fmt.Sprintf("batch insert %d rows into %s", total, store))
		_, err := dst.BatchInsertKVAndIndex(dstRows, store, nil)
		if err != nil {
			return 0, err
		}
		return total, err2
	} else {
		logger.Error("unable to get rows %v", err)
		return 0, err
	}
}

// CopyAll copies n rows from one db to another
func CopyStore(src, dst KVStore, batch int, store string) (int, error) {
	// logger.Info("copy %s", store)
	total := 0
	if grows, err := src.AllCursor(store); err == nil {
		rows := grows.(*common.CursorRows)
		defer rows.Close()
		for {
			dstRows := [][][]byte{}
			var err2 error
			for i := 0; i < batch; i++ {

				var kv [][]byte
				kv, err2 = rows.NextKV()
				if err2 != nil {
					break
				}
				dstRows = append(dstRows, kv)
				// logger.Debug("processing row ... %v", row)
			}
			total += len(dstRows)
			logger.Debug(fmt.Sprintf("batch insert %d rows into %s", len(dstRows), store))
			_, err := dst.BatchInsertKVAndIndex(dstRows, store, nil)
			if err != nil {
				return total, err
			}
			if err2 != nil {
				if err2 != gostore.ErrEOF {
					return total, err2
				}
				return total, nil
			}
		}
	} else {
		logger.Error("unable to get rows %v", err)
		return 0, err
	}

}

// Clone a store
func Clone(batchCount int, leftStore, rightStore, leftStorePath, rightStorePath string, stores []string) error {
	var src, dst KVStore
	var err error
	switch leftStore {
	case "bolt":
		src, err = bolt.NewDBOnly(leftStorePath)
		if err != nil {
			return err
		}
	case "badger":
		src, err = badger.NewDBOnly(leftStorePath)
		if err != nil {
			return err
		}
	}
	defer src.Close()

	switch rightStore {
	case "bolt":
		dst, err = bolt.New(rightStorePath)
		if err != nil {
			return err
		}
	case "badger":

		if _, err := os.Stat(rightStorePath); os.IsNotExist(err) {
			os.Mkdir(rightStorePath, os.FileMode(0777))
		}
		indexPath := filepath.Join(rightStorePath, "db.index")
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			os.Mkdir(indexPath, os.FileMode(0777))
		}
		dst, err = badger.NewWithIndexer(rightStorePath, indexer.NewBadgerIndexer(indexPath))
		if err != nil {
			return err
		}
	}
	defer dst.Close()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Kill)

	for i := 0; i < len(stores); i++ {
		select {
		case <-quit:
			logger.Debug("quit")
			return nil
		default:
			store := stores[i]
			total, err := CopyStore(src, dst, batchCount, store)
			if err != nil {

				continue
			}
			if err := dst.(*badger.BadgerStore).Db.PurgeOlderVersions(); err != nil {
				logger.Warn("unable to purge %s", err.Error())
			}
			logger.Debug("copied %d rows from %s::%s to %s::%s", total, leftStore, store, rightStore, store)
		}
	}
	// os.
	return nil
}
