package badger

//TODO: Extract methods into functions
import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"encoding/json"
	"github.com/blevesearch/bleve"
	badgerdb "github.com/dgraph-io/badger"
	"github.com/mgutz/logxi/v1"
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
	Indexer     *indexer.Indexer
	tableConfig map[string]*TableConfig
}

// IndexedData represents a stored row
type IndexedData struct {
	Bucket string      `json:"bucket"`
	Data   interface{} `json:"data"`
}

// Type type o data
func (d *IndexedData) Type() string {
	return "indexed_data"
}

// New badger store
func New(p string) (s gostore.ObjectStore, err error) {
	path := p + "/db"
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}
	if err != nil {
		return
	}
	opt := badgerdb.DefaultOptions
	opt.Dir = path
	opt.ValueDir = opt.Dir
	opt.SyncWrites = true
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	indexMapping := bleve.NewIndexMapping()
	indexPath := path + ".index"
	index := indexer.NewIndexer(indexPath, indexMapping)
	s = BadgerStore{
		[]byte("_default"),
		db,
		index,
		make(map[string]*TableConfig)}
	//	e.CreateBucket(bucket)
	return
}

func (s BadgerStore) CreateDatabase() error {
	return nil
}

func (s BadgerStore) keyForTable(table string) string {
	return "t$" + table
}
func (s BadgerStore) keyForTableId(table, id string) string {
	return s.keyForTable(table) + "|" + id
}

func (s BadgerStore) CreateTable(table string, config interface{}) error {
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

func (s BadgerStore) GetStore() interface{} {
	return s.Db
}

func (s BadgerStore) CreateBucket(bucket string) error {
	return nil
}

func (s BadgerStore) updateTableStats(table string, change uint) {

}

func (s BadgerStore) _Get(key, store string) ([][]byte, error) {
	k := s.keyForTableId(store, key)
	storeKey := []byte(k)
	var val []byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		item, err2 := txn.Get(storeKey)
		if err2 != nil {
			logger.Info("error getting key", "key", k, "err", err2.Error())
			return err2
		}
		v, err2 := item.Value()
		if err2 != nil {
			return err2
		}
		val = make([]byte, len(v))
		copy(val, v)
		return nil
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
	logger.Debug("retrieved a key from badger table", "key", key, "storeKey", k)
	data := make([][]byte, 2)
	data[0] = []byte(key)
	data[1] = val
	return data, nil
}
func (s BadgerStore) _Delete(key, store string) error {
	storeKey := []byte(s.keyForTableId(store, key))
	err := s.Db.View(func(txn *badgerdb.Txn) error {
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
func (s BadgerStore) _Save(key, store string, data []byte) error {
	storeKey := []byte(s.keyForTableId(store, key))
	err := s.Db.Update(func(txn *badgerdb.Txn) error {
		err := txn.Set(storeKey, data, 0)
		return err
	})
	return err
}

// All gets all entries in a store
// a gouritine which holds open a db view transaction and then listens on
// a channel for getting the next row itr. There is also a timeout to prevent long running routines
func (s BadgerStore) All(count int, skip int, store string) (gostore.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			logger.Debug("key + " + string(k) + " retrieved")
			v, err := item.Value()
			if err != nil {
				return err
			}
			obj := make([][]byte, 2)
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			obj[1] = make([]byte, len(v))
			copy(obj[0], k)
			copy(obj[1], v)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs) - 1}, err
}

func (s BadgerStore) GetAll(count int, skip int, bucket []string) (objs [][][]byte, err error) {
	return nil, gostore.ErrNotImplemented
}

func (s BadgerStore) _Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	return nil, gostore.ErrNotImplemented
}

func (s BadgerStore) FilterSuffix(suffix []byte, count int, resource string) (objs [][]byte, err error) {
	return nil, gostore.ErrNotImplemented
}

func (s BadgerStore) StreamFilter(key []byte, count int, resource string) chan []byte {
	return nil
}

func (s BadgerStore) StreamAll(count int, resource string) chan [][]byte {
	return nil
}

func (s BadgerStore) Stats(bucket string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})

	return
}

func (s BadgerStore) AllCursor(store string) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

func (s BadgerStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

// Since get items after a key
func (s BadgerStore) Since(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		for it.Seek([]byte(s.keyForTableId(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			obj := make([][]byte, 2)
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			obj[1] = make([]byte, len(v))
			copy(obj[0], k)
			copy(obj[1], v)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs) - 1}, err
}

//Before Get all recent items from a key
func (s BadgerStore) Before(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Reverse = true
		it := txn.NewIterator(opts)
		for it.Seek([]byte(s.keyForTableId(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			obj := make([][]byte, 2)
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			obj[1] = make([]byte, len(v))
			copy(obj[0], k)
			copy(obj[1], v)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs) - 1}, err
} //Get all existing items before a key

func (s BadgerStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all recent items from a key
func (s BadgerStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all existing items before a key
func (s BadgerStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	return 0, gostore.ErrNotImplemented
} //Get all existing items before a key

func (s BadgerStore) Get(key string, store string, dst interface{}) error {
	data, err := s._Get(key, store)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}
func (s BadgerStore) SaveRaw(key string, val []byte, store string) error {
	if err := s._Save(key, store, val); err != nil {
		return err
	}
	return nil
}
func (s BadgerStore) Save(key, store string, src interface{}) (string, error) {
	data, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	if err := s._Save(key, store, data); err != nil {
		return "", err
	}
	// }
	err = s.Indexer.IndexDocument(key, IndexedData{store, src})
	return key, err
}
func (s BadgerStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, gostore.ErrNotImplemented
}
func (s BadgerStore) Update(key string, store string, src interface{}) error {
	//get existing
	var existing map[string]interface{}
	if err := s.Get(key, store, &existing); err != nil {
		return err
	}

	logger.Info("update", "Store", store, "data", src, "existing", existing)
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &existing); err != nil {
		return err
	}
	data, err = json.Marshal(existing)
	if err != nil {
		return err
	}
	if err := s._Save(key, store, data); err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(key, IndexedData{store, existing})
	return err
}
func (s BadgerStore) Replace(key string, store string, src interface{}) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	if err := s._Save(key, store, data); err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(key, IndexedData{store, src})
	return err
}
func (s BadgerStore) Delete(key string, store string) error {
	return s._Delete(key, store)
}

//Filter
func (s BadgerStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s BadgerStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s BadgerStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterGet", "filter", filter, "Store", store, "opts", opts)
	//check if filter contains a nested field which is used to traverse a sub bucket
	var (
		data [][]byte
	)

	// res, err := s.Indexer.Query(common.GetQueryString(store, filter))
	query := common.GetQueryString(store, filter)
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

// FilterGetAll get all from filter
func (s BadgerStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	q := common.GetQueryString(store, filter)
	logger.Info("FilterGetAll", "count", count, "skip", skip, "Store", store, "query", q)
	res, err := s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
	if err != nil {
		logger.Warn("err", "error", err)
		return nil, err
	}
	if res.Total == 0 {
		return nil, gostore.ErrNotFound
	}
	// return NewIndexedBadgerRows(store, res.Total, res, &s), nil
	return &SyncIndexRows{name: store, length: res.Total, result: res, bs: &s}, nil
}

func (s BadgerStore) FilterDelete(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterDelete", "filter", filter, "store", store)
	res, err := s.Indexer.Query(common.GetQueryString(store, filter))
	if err == nil {
		if res.Total == 0 {
			return gostore.ErrNotFound
		}
		// if res.Total > 1 {
		// 	return gostore.ErrDuplicatePk
		// }

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

func (s BadgerStore) FilterCount(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	res, err := s.Indexer.Query(common.GetQueryString(store, filter))
	if err != nil {
		return 0, err
	}
	if res.Total == 0 {
		return 0, gostore.ErrNotFound
	}
	return int64(res.Total), nil
}

//Misc gets
func (s BadgerStore) GetByField(name, val, store string, dst interface{}) error { return nil }
func (s BadgerStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return gostore.ErrNotImplemented
}

func (s BadgerStore) BatchDelete(ids []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	return gostore.ErrNotImplemented
}

func (s BadgerStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s BadgerStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s BadgerStore) BatchInsert(data []interface{}, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(data))
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
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
				return err
			}
			err = txn.Set([]byte(key), data, 0)
			if err != nil {
				return err
			}
			b.Index(key, IndexedData{store, src})
			if err2 := s.Indexer.IndexDocument(key, IndexedData{store, src}); err2 != nil {
				logger.Warn(err.Error())
			}
			keys[i] = key
		}
		return s.Indexer.Batch(b)
	})
	return
}
func (s BadgerStore) Close() {
	s.Db.Close()
}
