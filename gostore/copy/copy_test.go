package copy

import (
	"fmt"
	"os"

	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/bolt"

	"github.com/osiloke/gostore-contrib/badger"
	"github.com/osiloke/gostore-contrib/indexer"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

var mode = int(0777)

func tempPath() string {
	// Retrieve a temporary path.
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(fmt.Sprintf("temp file: %s", err))
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func makeBoltDb(boltPath, indexPath string) *bolt.BoltStore {
	DB, err := bolt.NewWithPaths(boltPath, indexPath)
	if err != nil {
		panic(err)
	}
	return DB

}
func makeBadgerDb(path string) *badger.BadgerStore {
	os.Mkdir(path, os.FileMode(mode))
	indexPath := path + ".index"
	dbPath := path + "/db"
	os.RemoveAll(dbPath)
	os.RemoveAll(indexPath)
	os.Mkdir(dbPath, os.FileMode(mode))
	os.Mkdir(indexPath, os.FileMode(mode))
	_db, err := badger.NewWithIndexer(path, indexer.NewBadgerIndexer(indexPath))
	// _db, err := badger.New(path)
	if err != nil {
		panic(err)
	}
	return _db
}

func openBadgerDb(path string) *badger.BadgerStore {
	indexPath := path + ".index"
	_db, err := badger.NewWithIndexer(path, indexer.NewBadgerIndexer(indexPath))
	// _db, err := badger.New(path)
	if err != nil {
		panic(err)
	}
	return _db
}
func countRows(rows gostore.ObjectRows) int {
	c := 0
	for {
		_, ok := rows.NextRaw()
		if !ok {
			break
		}
		c++
	}
	return c
}
func TestCopyRowsBoltToBadger(t *testing.T) {
	boltPath := tempPath()
	boltIndexPath := tempPath()
	badgerPath := tempPath()
	src := makeBoltDb(boltPath, boltIndexPath)
	dst := makeBadgerDb(badgerPath)
	defer func() {
		src.Close()
		dst.Close()
		os.Remove(boltPath)
		os.Remove(boltIndexPath)
		os.Remove(badgerPath)
	}()
	store := "store"
	src.CreateTable(store, nil)
	// Only pass t into top-level Convey calls
	rows := []interface{}{
		map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
			"type":  "data",
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
			"type":  "data",
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
			"type":  "data",
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
			"type":  "data",
		},
	}
	copyCount := 2
	// save a bunch of dataa
	_, err := src.BatchInsert(rows, store, nil)
	assert.Nil(t, err, "no issues copying rows")
	_, err = CopyRows(src, dst, copyCount, store)
	assert.Nil(t, err, "no error encountered while copying store")
	if err != nil {
		return
	}
	dst.Close()
	dst = openBadgerDb(badgerPath)
	rrows, err := dst.FilterGetAll(map[string]interface{}{"type": "data"}, 10, 0, store, nil)
	assert.Nil(t, err, "error encountered while returning all entries in destination store")
	if err != nil {
		return
	}
	assert.Equal(t, copyCount, countRows(rrows), "total of new store should equal total of rows copied from src store")
}
func TestCopyAllBoltToBadger(t *testing.T) {
	boltPath := tempPath()
	boltIndexPath := tempPath()
	badgerPath := tempPath()
	src := makeBoltDb(boltPath, boltIndexPath)
	dst := makeBadgerDb(badgerPath)
	defer func() {
		src.Close()
		dst.Close()
		os.Remove(boltPath)
		os.Remove(boltIndexPath)
		os.Remove(badgerPath)
	}()
	store := "store"
	src.CreateTable(store, nil)
	// Only pass t into top-level Convey calls
	rows := []interface{}{
		map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
		},
	}
	batchCount := 2
	// save a bunch of dataa
	_, err := src.BatchInsert(rows, store, nil)
	assert.Nil(t, err, "no issues copying rows")
	_, err = CopyStore(src, dst, batchCount, store)
	assert.Nil(t, err, "no error encountered while copying store")
	rrows, err := dst.All(len(rows), 0, store)
	assert.Nil(t, err, "no error encountered while returning all entries in destination store")
	assert.Equal(t, len(rows), countRows(rrows), "total of new store should equal total of rows copied from src store")
}
