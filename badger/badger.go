package badger

//TODO: Extract methods into functions
import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	// "fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blevesearch/bleve"
	badgerdb "github.com/dgraph-io/badger"
	"github.com/gosexy/to"
	log "github.com/mgutz/logxi/v1"
	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/common"
	"github.com/osiloke/gostore-contrib/indexer"
)

var logger = log.New("gostore-contrib.badger")

type HasID interface {
	GetId() string
}

// TableConfig cofig for table
type TableConfig struct {
	NestedBucketFieldMatcher map[string]*regexp.Regexp //defines fields to be used to extract nested buckets for data
}

// BadgerStore gostore implementation that used badgerdb
type BadgerStore struct {
	Bucket      []byte
	Db          *badgerdb.DB
	Indexer     indexer.Indexer
	tableConfig map[string]*TableConfig
	t           *time.Ticker
	done        chan bool
}

// IndexedData represents a stored row
type IndexedData struct {
	Bucket  string      `json:"bucket"`
	GeoData interface{} `json:"location,omitempty"`
	Data    interface{} `json:"data"`
}

// Type type o data
func (d *IndexedData) Type() string {
	return "indexed_data"
}

func (s *BadgerStore) setupTicker() {
	done := make(chan bool)
	ticker := time.NewTicker(5 * time.Minute)
	s.done = done
	s.t = ticker
	go func() {
		for range ticker.C {
		again:
			err := s.Db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
		done <- true
	}()
	logger.Debug("setup ticker")
}
func NewDBOnly(dbPath string) (s *BadgerStore, err error) {
	opt := badgerdb.DefaultOptions
	opt.Dir = dbPath
	opt.ValueDir = opt.Dir
	opt.SyncWrites = true
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		[]byte("_default"),
		db,
		nil,
		make(map[string]*TableConfig),
		nil,
		nil,
	}
	s.setupTicker()
	return
}

// New badger store
func New(root string) (s *BadgerStore, err error) {
	dbPath := filepath.Join(root, "db")
	logger.Debug("New badgerdb", "path", dbPath)
	indexPath := filepath.Join(root, "db.index")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {

		os.Mkdir(dbPath, os.FileMode(0600))
		logger.Debug("made badger db", "path", dbPath)
	}

	opt := badgerdb.DefaultOptions
	opt.Dir = dbPath
	opt.ValueDir = dbPath
	// opt.SyncWrites = true
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = false
	indexMapping.StoreDynamic = false
	index := indexer.NewIndexer(indexPath, indexMapping)
	s = &BadgerStore{
		[]byte("_default"),
		db,
		index,
		make(map[string]*TableConfig),
		nil,
		nil,
	}
	s.setupTicker()
	return
}

//NewWithIndexer New badger store with indexer
func NewWithIndexer(root string, index indexer.Indexer) (s *BadgerStore, err error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0755))
		logger.Debug("created root path " + root)
	}
	dbPath := filepath.Join(root, "db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		os.Mkdir(dbPath, os.FileMode(0755))
		logger.Debug("created badger directory " + dbPath)
	}

	opt := badgerdb.DefaultOptions
	opt.Dir = dbPath
	opt.ValueDir = dbPath
	opt.SyncWrites = true
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		[]byte("_default"),
		db,
		index,
		make(map[string]*TableConfig),
		nil,
		nil,
	}
	s.setupTicker()
	//	e.CreateBucket(bucket)
	return
}

// NewWithIndex New badger store with indexer
func NewWithIndex(root, index string) (s *BadgerStore, err error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0755))
		logger.Debug("created root path " + root)
	}
	indexPath := filepath.Join(root, "db.index")
	var ix indexer.Indexer
	if index == "badger" {
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {

			os.Mkdir(indexPath, os.FileMode(0755))
			logger.Debug("made badger db index path", "path", indexPath)
		}
		ix = indexer.NewBadgerIndexer(indexPath)
		s, err = NewWithIndexer(root, ix)
	} else if index == "moss" {
		// var newMoss bool
		ix, _ = indexer.NewMossIndexer(indexPath)
		s, err = NewWithIndexer(root, ix)
		// if !newMoss {
		if err := indexer.ReIndex(s, ix); err != nil {
			return nil, err
		}
		// }
	} else if index == "geo-moss" {
		// var newMoss bool
		ix, _ = indexer.NewMossIndexer(indexPath)
		s, err = NewWithIndexer(root, ix)
		// if !newMoss {
		if err := indexer.ReIndex(s, ix); err != nil {
			return nil, err
		}
		// }
	} else {
		ix = indexer.NewDefaultIndexer(indexPath)
		s, err = NewWithIndexer(root, ix)
	}
	return
}

// NewWithGeoIndex New badger store with geo indexer
func NewWithGeoIndex(root, index, geoField, documentName, typefield string) (s *BadgerStore, err error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0755))
		logger.Debug("created root path " + root)
	}
	indexPath := filepath.Join(root, "db.index")
	var ix indexer.Indexer
	if index == "badger" {
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {

			os.Mkdir(indexPath, os.FileMode(0755))
			logger.Debug("made badger db index path", "path", indexPath)
		}
		ix = indexer.NewBadgerIndexer(indexPath)
		s, err = NewWithIndexer(root, ix)
	} else if index == "moss" {
		// var newMoss bool
		logger.Debug("new geo enabled indexer")
		ix, _ = indexer.NewMossIndexerWithGeoMapping(indexPath, geoField, indexer.NewGeoEnabledIndexMapping(geoField, documentName, typefield))
		s, err = NewWithIndexer(root, ix)

		ixj, _ := json.Marshal(ix.Index().Mapping())
		fmt.Println(string(ixj))
		// if !newMoss {
		if err := indexer.ReIndex(s, ix); err != nil {
			return nil, err
		}
		// }
	} else {
		ix = indexer.NewDefaultIndexer(indexPath)
		s, err = NewWithIndexer(root, ix)
	}
	return
}
func (s *BadgerStore) CreateDatabase() error {
	return nil
}

func (s *BadgerStore) keyForTable(table string) string {
	return "t$" + table
}
func (s *BadgerStore) keyForTableId(table, id string) string {
	return s.keyForTable(table) + "|" + id
}

func (s *BadgerStore) CreateTable(table string, config interface{}) error {
	//config used to configure table
	// if c, ok := config.(map[string]interface{}); ok {
	// 	if nested, ok := c["nested"]; ok {
	// 		nbfm := make(map[string]*regexp.Regexp)
	// 		for k, v := range nested.(map[string]interface{}) {
	// 			nbfm[k] = regexp.MustCompile(v.(string))
	// 		}
	// 		s.tableConfig[table] = &TableConfig{NestedBucketFieldMatcher: nbfm}
	// 	}
	// }
	return nil
}

func (s *BadgerStore) GetStore() interface{} {
	return s.Db
}

// UpdateTransaction starts an update transaction
func (s *BadgerStore) UpdateTransaction() gostore.Transaction {
	return &BadgerTransaction{s.Db, s.Db.NewTransaction(true), "update"}
}

// FinishTransaction ebds transaction
func (s *BadgerStore) FinishTransaction(tx gostore.Transaction) error {
	return tx.Commit()
}

func (s *BadgerStore) CreateBucket(bucket string) error {
	return nil
}

func (s *BadgerStore) updateTableStats(table string, change uint) {

}

func (s *BadgerStore) _Get(key, store string) ([][]byte, error) {
	k := s.keyForTableId(store, key)
	storeKey := []byte(k)
	var val []byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		item, err2 := txn.Get(storeKey)
		if err2 != nil {
			logger.Info("error getting key", "key", k, "err", err2.Error())
			return err2
		}
		err2 = item.Value(func(v []byte) error {
			val = append([]byte{}, v...)
			return nil
		})
		return err2
	})
	if err != nil {
		if err == badgerdb.ErrKeyNotFound {
			return nil, gostore.ErrNotFound
		}
		return nil, err
	}
	if len(val) == 0 {
		return nil, gostore.ErrNotFound
	}
	logger.Debug("_Get success", "key", key, "storeKey", k)
	data := make([][]byte, 2)
	data[0] = []byte(key)
	data[1] = val
	return data, nil
}

// DeleteByPrefix deletes entries by key prefix
// https://github.com/dgraph-io/badger/issues/598
func (s *BadgerStore) DeleteByPrefix(prefix []byte) {
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := s.Db.Update(func(txn *badgerdb.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	collectSize := 100000
	s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete = append(keysForDelete, key)
			keysCollected++
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					panic(err)
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				panic(err)
			}
		}

		return nil
	})
}

// DeleteByPrefix deletes entries by key prefix
// https://github.com/dgraph-io/badger/issues/598
func (s *BadgerStore) FilterDeleteByPrefix(store string) error {
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := s.Db.Update(func(txn *badgerdb.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	collectSize := 100000
	b := s.Indexer.BatchIndex()
	prefix := []byte(s.keyForTable(store))
	return s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete = append(keysForDelete, key)
			keysCollected++
			b.Delete(string(strings.Split(string(key), "|")[1]))
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					return err
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
				s.Indexer.Batch(b)
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				panic(err)
			}
			s.Indexer.Batch(b)
		}

		return nil
	})
}
func (s *BadgerStore) _Delete(key, store string) error {
	storeKey := []byte(s.keyForTableId(store, key))
	err := s.Db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(storeKey)
	})
	if err != nil {
		if err == badgerdb.ErrKeyNotFound {
			return gostore.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *BadgerStore) _Save(key, store string, data []byte) error {
	storeKey := []byte(s.keyForTableId(store, key))
	err := s.Db.Update(func(txn *badgerdb.Txn) error {
		logger.Debug("_Save", "key", key, "store", store, "storeKey", storeKey)
		err := txn.Set(storeKey, data)
		return err
	})
	return err
}

// All gets all entries in a store
// a gouritine which holds open a db view transaction and then listens on
// a channel for getting the next row itr. There is also a timeout to prevent long running routines
func (s *BadgerStore) All(count int, skip int, store string) (gostore.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = count / 2
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			obj := make([][]byte, 2)
			// logger.Debug("key + " + string(k) + " retrieved")
			err := item.Value(func(v []byte) error {
				obj[1] = append([]byte{}, v...)
				return nil
			})
			if err != nil {
				return err
			}
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			copy(obj[0], k)
		}
		return nil
	})
	if len(objs) > 0 {
		return &TransactionRows{entries: objs, length: len(objs)}, err
	}
	return nil, gostore.ErrNotFound
}

func (s *BadgerStore) GetAll(count int, skip int, bucket []string) (objs [][][]byte, err error) {
	return nil, gostore.ErrNotImplemented
}

func (s *BadgerStore) _Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	return nil, gostore.ErrNotImplemented
}

func (s *BadgerStore) FilterSuffix(suffix []byte, count int, resource string) (objs [][]byte, err error) {
	return nil, gostore.ErrNotImplemented
}

func (s *BadgerStore) StreamFilter(key []byte, count int, resource string) chan []byte {
	return nil
}

func (s *BadgerStore) StreamAll(count int, resource string) chan [][]byte {
	return nil
}

func (s *BadgerStore) Stats(bucket string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})

	return
}

func (s *BadgerStore) Cursor() (common.Iterator, error) {
	rv := Iterator{
		iterator: s.Db.NewTransaction(false).NewIterator(badgerdb.DefaultIteratorOptions),
	}
	rv.iterator.Rewind()
	return &rv, nil
}

func (s *BadgerStore) AllCursor(store string) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

func (s *BadgerStore) Stream() (*common.CursorRows, error) {
	return nil, gostore.ErrNotImplemented
}

func (s *BadgerStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

// Since get items after a key
func (s *BadgerStore) Since(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(s.keyForTableId(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			obj := make([][]byte, 2)
			err := item.Value(func(v []byte) error {
				obj[1] = append([]byte{}, v...)
				return nil
			})
			if err != nil {
				return err
			}
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			copy(obj[0], k)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs)}, err
}

//Before Get all recent items from a key
func (s *BadgerStore) Before(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(s.keyForTableId(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			obj := make([][]byte, 2)
			err := item.Value(func(v []byte) error {
				obj[1] = append([]byte{}, v...)
				return nil
			})
			if err != nil {
				return err
			}
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			copy(obj[0], k)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs)}, err
} //Get all existing items before a key

func (s *BadgerStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all recent items from a key
func (s *BadgerStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all existing items before a key
func (s *BadgerStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	return 0, gostore.ErrNotImplemented
} //Get all existing items before a key

func (s *BadgerStore) Get(key string, store string, dst interface{}) error {
	data, err := s._Get(key, store)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}

func (s *BadgerStore) SaveRaw(key string, val []byte, store string) error {
	if err := s._Save(key, store, val); err != nil {
		return err
	}
	return nil
}
func (s *BadgerStore) Save(key, store string, src interface{}) (string, error) {
	data, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	skey := s.keyForTableId(store, key)
	storeKey := []byte(skey)
	logger.Debug("Save", "key", key, "store", store, "storeKey", skey)
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		err := txn.Set(storeKey, data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
	})
	return key, err
}

// SaveWithGeo save
func (s *BadgerStore) SaveWithGeo(key, store string, src interface{}, field string) (string, error) {
	if srcMap, ok := src.(map[string]interface{}); ok {
		skey := s.keyForTableId(store, key)
		storeKey := []byte(skey)
		logger.Debug("SaveWithGeo", "key", key, "store", store, "storeKey", skey)
		err := s.Db.Update(func(txn *badgerdb.Txn) error {
			if len(field) > 0 {
				geo, err := valForPath(field, srcMap)
				if err == nil {
					srcMap["_location"] = geo

					data, err := json.Marshal(srcMap)
					if err != nil {
						return err
					}
					err = txn.Set(storeKey, data)
					if err != nil {
						return err
					}
					return s.Indexer.IndexDocument(key, map[string]interface{}{"bucket": store, "data": srcMap, "location": geo})
				}
				return err
			}

			data, err := json.Marshal(src)
			if err != nil {
				return err
			}
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			return s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
		})
		return "", err
	}
	return key, errors.New("unable to save")
}

// SaveWithGeoTX save a key within a transaction
func (s *BadgerStore) SaveWithGeoTX(key, store string, src interface{}, field string, txn gostore.Transaction) error {
	if srcMap, ok := src.(map[string]interface{}); ok {
		skey := s.keyForTableId(store, key)
		storeKey := []byte(skey)
		logger.Debug("SaveWithGeoTX", "key", key, "store", store, "storeKey", skey)
		if len(field) > 0 {
			geo, err := valForPath(field, srcMap)
			if err == nil {
				srcMap["_location"] = geo
				data, err := json.Marshal(srcMap)
				if err != nil {
					return err
				}
				err = txn.Set(storeKey, data)
				if err != nil {
					return err
				}
				return s.Indexer.IndexDocument(key, map[string]interface{}{"bucket": store, "data": srcMap, "location": geo})
			}
			return err

		}

		data, err := json.Marshal(src)
		if err != nil {
			return err
		}
		err = txn.Set(storeKey, data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
	}
	return errors.New("unable to save")
}

// SaveTX save a key within a transaction
func (s *BadgerStore) SaveTX(key, store string, src interface{}, txn gostore.Transaction) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	skey := s.keyForTableId(store, key)
	storeKey := []byte(skey)
	logger.Debug("SaveTX", "key", key, "store", store, "storeKey", skey)
	err = txn.Set(storeKey, data)
	if err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
	return err
}

// GetTX get a key within a transaction
func (s *BadgerStore) GetTX(key string, store string, dst interface{}, txn gostore.Transaction) error {
	k := s.keyForTableId(store, key)
	storeKey := []byte(k)
	var val []byte
	val, err := txn.Get(storeKey)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return gostore.ErrNotFound
	}
	logger.Debug("GetTX success", "key", key, "storeKey", k)
	data := make([][]byte, 2)
	data[0] = []byte(key)
	data[1] = val
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}

func (s *BadgerStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, gostore.ErrNotImplemented
}
func (s *BadgerStore) Update(key string, store string, src interface{}) error {
	return gostore.ErrNotImplemented
	// //get existing
	// var existing map[string]interface{}
	// if err := s.Get(key, store, &existing); err != nil {
	// 	return err
	// }

	// logger.Info("update", "Store", store, "data", src, "existing", existing)
	// data, err := json.Marshal(src)
	// if err != nil {
	// 	return err
	// }
	// if err := json.Unmarshal(data, &existing); err != nil {
	// 	return err
	// }
	// data, err = json.Marshal(existing)
	// if err != nil {
	// 	return err
	// }
	// if err := s._Save(key, store, data); err != nil {
	// 	return err
	// }
	// err = s.Indexer.IndexDocument(key, IndexedData{store, existing})
	// return err
}
func (s *BadgerStore) Replace(key string, store string, src interface{}) error {
	_, err := s.Save(key, store, src)
	return err
}
func (s *BadgerStore) ReplaceTX(key string, store string, src interface{}, tx gostore.Transaction) error {
	return s.SaveTX(key, store, src, tx)
}
func (s *BadgerStore) DeleteTX(key string, store string, tx gostore.Transaction) error {
	skey := s.keyForTableId(store, key)
	storeKey := []byte(skey)
	logger.Info("DeleteTX", "key", key)
	return tx.Delete(storeKey)
}
func (s *BadgerStore) Delete(key string, store string) error {
	return s._Delete(key, store)
}

//Filter
func (s *BadgerStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s *BadgerStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s *BadgerStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterGet", "filter", filter, "Store", store, "opts", opts)
	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		var (
			data [][]byte
		)

		// res, err := s.Indexer.Query(indexer.GetQueryString(store, filter))
		query := indexer.GetQueryString(store, query)
		res, err := s.Indexer.QueryWithOptions(query, 1, 0, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Info("FilterGet failed", "query", query)
			return err
		}
		logger.Info("FilterGet success", "query", query)
		if res.Total == 0 {
			logger.Info("FilterGet empty result", "result", res.String())
			return gostore.ErrNotFound
		}
		data, err = s._Get(res.Hits[0].ID, store)
		if err != nil {
			return err
		}

		err = json.Unmarshal(data[1], dst)
		return err
	}
	return gostore.ErrNotFound

}
func (s *BadgerStore) FilterGetTX(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions, tx gostore.Transaction) error {
	logger.Info("FilterGetTX", "filter", filter, "Store", store, "opts", opts)
	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		// res, err := s.Indexer.Query(indexer.GetQueryString(store, filter))
		query := indexer.GetQueryString(store, query)
		res, err := s.Indexer.QueryWithOptions(query, 1, 0, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Info("FilterGetTX failed", "query", query)
			return err
		}
		logger.Info("FilterGetTX success", "query", query)
		if res.Total == 0 {
			logger.Info("FilterGetTX empty result", "result", res.String())
			return gostore.ErrNotFound
		}
		key := res.Hits[0].ID
		k := s.keyForTableId(store, key)
		storeKey := []byte(k)
		data, err := tx.Get(storeKey)
		if err != nil {
			return err
		}

		err = json.Unmarshal(data, dst)
		return err
	}
	return gostore.ErrNotFound

}

// FilterGetAll allows you to filter a store if an indexer exists
func (s *BadgerStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	if query, ok := filter["q"].(map[string]interface{}); ok {
		q := indexer.GetQueryString(store, query)
		logger.Info("FilterGetAll", "count", count, "skip", skip, "Store", store, "query", q)
		res, err := s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Warn("err", "error", err, "res")
			return nil, err
		}
		if res.Total == 0 {
			return nil, gostore.ErrNotFound
		}
		// return NewIndexedBadgerRows(store, res.Total, res, &s), nil
		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, nil
	}
	return nil, gostore.ErrNotFound
}

func (s *BadgerStore) Query(query, aggregates map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, gostore.AggregateResult, error) {
	if len(query) > 0 {
		var err error
		var res *bleve.SearchResult
		agg := gostore.AggregateResult{}
		q := indexer.GetQueryString(store, query)
		var order indexer.RequestOpt
		if orderBy := opts.GetOrderBy(); len(orderBy) > 0 {
			order = indexer.OrderRequest((orderBy))
		} else {
			order = indexer.OrderRequest([]string{"-_score", "-_id"})
		}
		if len(aggregates) == 0 {
			logger.Info("Query", "count", count, "skip", skip, "Store", store, "query", q, "order", order)
			res, err = s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, order)

		} else {
			facets := indexer.Facets{}
			for k, v := range aggregates {
				if k == "top" && v != nil {
					facets.Top = make(map[string]indexer.TopFacet)
					for kk, vv := range v.(map[string]interface{}) {
						f := vv.(map[string]interface{})
						name := kk
						if n, ok := f["name"].(string); ok {
							name = n
						}
						facets.Top[name] = indexer.TopFacet{
							Name:  f["name"].(string),
							Field: "data." + f["field"].(string),
							Count: int(to.Int64(f["count"])),
						}
					}
				}
				if k == "range" && v != nil {
					facets.Range = make(map[string]indexer.RangeFacet)
					if ranges, ok := v.(map[string]interface{}); ok {
						for kk, vv := range ranges {
							f := vv.(map[string]interface{})
							name := kk
							facets.Range[name] = indexer.RangeFacet{
								Field:  "data." + f["field"].(string),
								Ranges: f["ranges"].([]interface{}),
							}
						}
					} else if ranges, ok := v.([]interface{}); ok {
						for _, vv := range ranges {
							f := vv.(map[string]interface{})
							name := f["name"].(string)
							facets.Range[name] = indexer.RangeFacet{
								Field:  "data." + f["field"].(string),
								Ranges: f["ranges"].([]interface{}),
							}
						}
					}
				}
			}
			logger.Info("Query", "count", count, "skip", skip, "Store", store, "query", q, "facets", facets, "orderBy", order)
			res, err = s.Indexer.FacetedQuery(q, &facets, count, skip, true, []string{}, order)

		}
		if err != nil {
			logger.Warn("err", "error", err)
			return nil, nil, err
		}
		if len(res.Facets) > 0 {
			for k, v := range res.Facets {
				if len(v.NumericRanges) > 0 {
					// numericRanges := make([]interface{}, len(v.NumericRanges))
					// for i, n := range v.NumericRanges{
					// 	numericRanges[i] = map[string]interface{}{"field": n.Name, "min": n.Min, "max": n.Max, "count": n.Count}
					// }
					agg[k] = gostore.Match{
						NumberRange: v.NumericRanges,
						Field:       strings.SplitN(v.Field, ".", 2)[1],
						Matched:     v.Total,
						UnMatched:   v.Other,
						Missing:     v.Missing,
					}
				} else {
					agg[k] = gostore.Match{
						Top:       v.Terms,
						Field:     strings.SplitN(v.Field, ".", 2)[1],
						Matched:   v.Total,
						UnMatched: v.Other,
						Missing:   v.Missing,
					}
				}
			}
		}
		if res.Total == 0 {
			return nil, agg, gostore.ErrNotFound
		}

		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, agg, err
	}
	return nil, nil, gostore.ErrNotFound
}

// GeoQuery query a geocapable indexer
func (s *BadgerStore) GeoQuery(lon, lat float64, distance string, query map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {

	var err error
	var res *bleve.SearchResult
	q := "*"
	if len(query) > 0 {
		q = indexer.GetQueryString(store, query)
		// if len(aggregates) == 0 {
	}
	logger.Info("GeoQuery", "count", count, "skip", skip, "Store", store, "lat", lat, "lon", lon, "distance", distance, "query", q)
	if geoIndexer, ok := s.Indexer.(indexer.GeoCapableIndexer); ok {
		res, err = geoIndexer.GeoDistanceQuery(q, lon, lat, distance, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
	} else {
		return nil, gostore.ErrNotImplemented
	}
	if err != nil {
		logger.Warn("err", "error", err)
		return nil, err
	}
	if res.Total == 0 {
		return nil, gostore.ErrNotFound
	}

	return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, err
}

// FilterDelete filter delete items
func (s *BadgerStore) FilterDelete(query map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterDelete", "filter", query, "store", store)
	count := 1000
	res, err := s.Indexer.Query(indexer.GetQueryString(store, query))
	res, err = s.Indexer.QueryWithOptions(indexer.GetQueryString(store, query), count, 0, true, []string{})
	if err == nil {
		if res.Total == 0 {
			return gostore.ErrNotFound
		}
		for _, v := range res.Hits {
			err = s._Delete(v.ID, store)
			if err != nil {
				break
			}
			err = s.Indexer.UnIndexDocument(v.ID)
			if err != nil {
				break
			}
		}
	}
	return err
}

func (s *BadgerStore) FilterCount(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	if query, ok := filter["q"].(map[string]interface{}); ok {
		res, err := s.Indexer.Query(indexer.GetQueryString(store, query))
		if err != nil {
			return 0, err
		}
		if res.Total == 0 {
			return 0, gostore.ErrNotFound
		}
		return int64(res.Total), nil
	}
	return 0, gostore.ErrNotFound
}

//Misc gets
func (s *BadgerStore) GetByField(name, val, store string, dst interface{}) error { return nil }
func (s *BadgerStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return gostore.ErrNotImplemented
}

func (s *BadgerStore) BatchDelete(ids []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	return gostore.ErrNotImplemented
}

func (s *BadgerStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	// keys = make([]string, len(data))
	b := s.Indexer.BatchIndex()
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		for _, src := range data {
			var key string
			if _v, ok := src.(map[string]interface{}); ok {
				if k, ok := _v["id"].(string); ok {
					key = k
				} else {
					key = gostore.NewObjectId().String()
					_v["id"] = key
				}
			} else if _v, ok := src.(HasID); ok {
				key = _v.GetId()
			} else {
				key = gostore.NewObjectId().String()
			}
			data, err := json.Marshal(src)
			if err != nil {
				return err
			}
			storeKey := []byte(s.keyForTableId(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			indexedData := map[string]interface{}{"bucket": store, "data": src}
			logger.Debug("BatchInsert", "row", indexedData)
			b.Index(key, indexedData)
		}
		return s.Indexer.Batch(b)
	})
	return
}

func (s *BadgerStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s *BadgerStore) BatchInsert(data []interface{}, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(data))
	b := s.Indexer.BatchIndex()
	// err = s.Db.Update(func(txn *badgerdb.Txn) error {
	// 	for i, src := range data {
	// 		var key string
	// 		if _v, ok := src.(map[string]interface{}); ok {
	// 			if k, ok := _v["id"].(string); ok {
	// 				key = k
	// 			} else {
	// 				key = gostore.NewObjectId().String()
	// 				_v["id"] = key
	// 			}
	// 		} else if _v, ok := src.(HasID); ok {
	// 			key = _v.GetId()
	// 		} else {
	// 			key = gostore.NewObjectId().String()
	// 		}
	// 		data, err := json.Marshal(src)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		storeKey := []byte(s.keyForTableId(store, key))
	// 		err = txn.Set(storeKey, data)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		indexedData := IndexedData{store, src}
	// 		logger.Debug("BatchInsert", "row", indexedData)
	// 		b.Index(key, indexedData)
	// 		keys[i] = key
	// 	}
	// 	return s.Indexer.Batch(b)
	// })
	txn := s.Db.NewTransaction(true)
	defer txn.Discard()
	for i, src := range data {
		var key string
		if _v, ok := src.(map[string]interface{}); ok {
			if k, ok := _v["id"].(string); ok {
				key = k
			} else {
				key = gostore.NewObjectId().String()
				_v["id"] = key
			}
		} else if _v, ok := src.(HasID); ok {
			key = _v.GetId()
		} else {
			key = gostore.NewObjectId().String()
		}
		data, err := json.Marshal(src)
		if err != nil {
			return nil, err
		}
		storeKey := []byte(s.keyForTableId(store, key))
		err = txn.Set(storeKey, data)
		if err != nil {
			return nil, err
		}
		indexedData := IndexedData{Bucket: store, Data: src}
		logger.Debug("BatchInsert", "row", indexedData)
		b.Index(key, indexedData)
		keys[i] = key
	}
	if err2 := txn.Commit(); err2 != nil {
		return nil, err2
	}
	err = s.Indexer.Batch(b)
	return
}

func (s *BadgerStore) BatchInsertTX(data []interface{}, store string, opts gostore.ObjectStoreOptions, txn gostore.Transaction) (keys []string, err error) {
	keys = make([]string, len(data))
	b := s.Indexer.BatchIndex()
	for i, src := range data {
		var key string
		if _v, ok := src.(map[string]interface{}); ok {
			if k, ok := _v["id"].(string); ok {
				key = k
			} else {
				key = gostore.NewObjectId().String()
				_v["id"] = key
			}
		} else if _v, ok := src.(HasID); ok {
			key = _v.GetId()
		} else {
			key = gostore.NewObjectId().String()
		}
		data, err := json.Marshal(src)
		if err != nil {
			return nil, err
		}
		storeKey := []byte(s.keyForTableId(store, key))
		err = txn.Set(storeKey, data)
		if err != nil {
			return nil, err
		}
		indexedData := IndexedData{Bucket: store, Data: src}
		logger.Debug("BatchInsertTX", "row", indexedData)
		b.Index(key, indexedData)
		keys[i] = key
	}
	if err2 := txn.Commit(); err2 != nil {
		return nil, err2
	}
	if err = txn.Restart(); err != nil {
		return
	}
	err = s.Indexer.Batch(b)
	return
}

func (s *BadgerStore) BatchInsertKVAndIndex(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(rows))
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		b := s.Indexer.BatchIndex()
		for i, row := range rows {
			key := string(row[0])
			data := row[1]
			storeKey := []byte(s.keyForTableId(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			var iData map[string]interface{}
			err := json.Unmarshal(row[1], &iData)

			if err != nil {
				return err
			}
			b.Index(key, IndexedData{Bucket: store, Data: iData})
			keys[i] = key
		}
		logger.Debug("copied", "rows", len(keys))
		return s.Indexer.Batch(b)
	})
	return
}
func (s *BadgerStore) BatchInsertKV(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(rows))
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		for i, row := range rows {
			key := string(row[0])
			data := row[1]
			storeKey := []byte(s.keyForTableId(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			// dataAsStr := string(data)
			keys[i] = key
		}
		logger.Debug("copied", "rows", len(keys))
		return nil
	})
	return
}
func (s *BadgerStore) Close() {
	defer s.t.Stop()
	if s.Db != nil {
		s.Db.Close()
		logger.Debug("closed badger store")
	}
	if s.Indexer != nil {
		s.Indexer.Close()
		logger.Debug("closed badger index")
	}
}
