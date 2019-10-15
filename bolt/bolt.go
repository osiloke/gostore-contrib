package bolt

//TODO: Extract methods into functions
import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	boltdb "github.com/boltdb/bolt"
	"github.com/gin-gonic/gin"
	log "github.com/mgutz/logxi/v1"
	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/common"
	"github.com/osiloke/gostore-contrib/indexer"
)

var logger = log.New("gostore-contrib.bolt")

// HasID defines stores that implement GetId
type HasID interface {
	GetId() string
}

// TableConfig config for store
type TableConfig struct {
	NestedBucketFieldMatcher map[string]*regexp.Regexp //defines fields to be used to extract nested buckets for data
}

// BoltStore a store with boltdb backend
type BoltStore struct {
	Bucket      []byte
	Db          *boltdb.DB
	Indexer     indexer.Indexer
	tableConfig map[string]*TableConfig
}

// IndexedData indexed data stored
type IndexedData struct {
	Bucket string      `json:"bucket"`
	Data   interface{} `json:"data"`
}

// Type implements HasType
func (d *IndexedData) Type() string {
	return "indexed_data"
}

// NewDBOnly creates only a db store, no index
func NewDBOnly(dbPath string) (store *BoltStore, err error) {
	var db *boltdb.DB
	db, err = boltdb.Open(dbPath, 0600, nil)
	if err != nil {
		return
	}
	store = &BoltStore{[]byte("_default"), db, nil, make(map[string]*TableConfig)}
	return
}

// NewWithPaths creates store with index at specified paths
func NewWithPaths(boltPath, indexPath string) (store *BoltStore, err error) {
	var db *boltdb.DB
	db, err = boltdb.Open(boltPath, 0600, nil)
	if err != nil {
		return
	}
	indexMapping := bleve.NewIndexMapping()
	index := indexer.NewIndexer(indexPath, indexMapping)
	store = &BoltStore{[]byte("_default"), db, index, make(map[string]*TableConfig)}
	return
}

func new(path string) (store *BoltStore, err error) {
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}

	if err != nil {
		return
	}
	boltPath := filepath.Join(path, "db")
	indexPath := filepath.Join(path, "db.index")
	store, err = NewWithPaths(boltPath, indexPath)
	return
}

func NewWithIndex(boltPath string, index indexer.Indexer) (store *BoltStore, err error) {
	var db *boltdb.DB
	db, err = boltdb.Open(boltPath, 0600, nil)
	if err != nil {
		return
	}
	store = &BoltStore{[]byte("_default"), db, index, make(map[string]*TableConfig)}
	return
}

func New(dbRootFolder string) (*BoltStore, error) {
	return new(dbRootFolder)
}

func NewWithBackup(path string, backupURI string) (store *BoltStore, err error) {
	store, err = new(path)
	if err != nil {
		return
	}
	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		err := store.WriteToHTTP(c.Writer)
		if err != nil {
			http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
		}
	})
	go router.Run(backupURI)

	return
}

func (s *BoltStore) CreateDatabase() error {
	return nil
}

func (s *BoltStore) CreateTable(table string, config interface{}) error {
	//config used to configure table
	s.CreateBucket(table)
	if c, ok := config.(map[string]interface{}); ok {
		if nested, ok := c["nested"]; ok {
			nbfm := make(map[string]*regexp.Regexp)
			for k, v := range nested.(map[string]interface{}) {
				nbfm[k] = regexp.MustCompile(v.(string))
			}
			s.tableConfig[table] = &TableConfig{NestedBucketFieldMatcher: nbfm}
		}
	}
	return nil
}

func (s *BoltStore) GetStore() interface{} {
	return s.Db
}

func (s *BoltStore) getBucketPath(bucket string) []string {
	return strings.Split("bucket", "/")
}
func (s *BoltStore) CreateBucket(bucket string) error {
	return s.Db.Update(func(tx *boltdb.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	})
}

func getBucket(tx *boltdb.Tx, bucket []string) (nbkt *boltdb.Bucket, err error) {
	bkt := tx.Bucket([]byte(bucket[0]))
	for _, k := range bucket[1:] {
		nbkt, err = bkt.CreateBucketIfNotExists([]byte(k))
		if err != nil {
			return
		}
	}
	return
}
func Get(key []byte, bucket []byte, db *boltdb.DB) (v []byte, err error) {
	defer timeTrack(time.Now(), "Bolt Store::Get "+string(key)+" from "+string(bucket))
	err = db.View(func(tx *boltdb.Tx) error {
		b := tx.Bucket(bucket)
		v = b.Get(key)
		if v == nil {
			return gostore.ErrNotFound
		}
		return nil
	})
	return
}

func NestedGet(key []byte, bucket []string, db *boltdb.DB) (v []byte, err error) {
	defer timeTrack(time.Now(), "Bolt Store::NestedGet "+string(key)+" from "+string(bucket[0]))
	err = db.View(func(tx *boltdb.Tx) error {
		nbkt, err := getBucket(tx, bucket)
		if err != nil {
			return err
		}
		v = nbkt.Get(key)
		if v == nil {
			return gostore.ErrNotFound
		}
		return nil
	})
	return
}

func PrefixGet(prefix []byte, bucket []byte, db *boltdb.DB) (k, v []byte, err error) {
	defer timeTrack(time.Now(), "Bolt Store::PrefixGet "+string(prefix)+" from "+string(bucket))
	err = db.View(func(tx *boltdb.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		k, v = c.Seek(prefix)
		if v == nil {
			return gostore.ErrNotFound
		}
		return nil
	})
	return
}

func (s *BoltStore) getBucketList(store string, data map[string]interface{}) (bucket []string, err error) {
	if config, ok := s.tableConfig[store]; ok {
		for nestedField, re := range config.NestedBucketFieldMatcher {
			if nestedFieldValue, ok := data[nestedField]; ok {
				bucket = []string{store}
				for _, match := range re.FindAllString(nestedFieldValue.(string), -1) {
					bucket = append(bucket, match)
				}
				return
			}
		}
	}
	err = ErrNoNestedBuckets
	return
}

func (s *BoltStore) _Get(key, resource string) (v [][]byte, err error) {
	logger.Info("_Get", "key", key, "bucket", resource)
	_key := []byte(key)
	vv, err := Get(_key, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{_key, vv}
	}
	return
}

func (s *BoltStore) _PrefixGet(prefix []byte, resource string) (v [][]byte, err error) {
	kk, vv, err := PrefixGet(prefix, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{kk, vv}
	}
	return
}

func (s *BoltStore) _Save(key []byte, data []byte, resource string) error {
	err := s.Db.Batch(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Put(key, data)
		return err
	})
	return err
}
func (s *BoltStore) _SaveTx(key []byte, data []byte, resource string) func(tx *boltdb.Tx) error {
	return func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Put(key, data)
		return err
	}
}

func (s *BoltStore) _Delete(key string, resource string) error {
	logger.Info("_Delete", "key", key, "bucket", resource)
	err := s.Db.Batch(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete([]byte(key))
		return err
	})
	return err
}

func (s *BoltStore) DeleteAll(resource string) error {
	err := s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			b.Delete(k)
		}
		return nil
	})
	return err
}

func (s *BoltStore) _NestedGet(key []byte, bucket []string) (v [][]byte, err error) {
	_key := []byte(key)
	vv, err := NestedGet(_key, bucket, s.Db)
	if vv != nil {
		v = [][]byte{_key, vv}
	}
	return
}
func (s *BoltStore) _NestedSave(key []byte, data []byte, bucket []string) error {
	err := s.Db.Batch(func(tx *boltdb.Tx) error {
		nbkt, err := getBucket(tx, bucket)
		if err == nil {
			err = nbkt.Put(key, data)
		}
		return err
	})
	return err
}
func (s *BoltStore) _NestedDelete(key string, resource string) error {
	err := s.Db.Batch(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete([]byte(key))
		return err
	})
	return err
}

func (s *BoltStore) All(count int, skip int, store string) (gostore.ObjectRows, error) {
	s.CreateBucket(store)
	_rows, err := s.GetAll(count, skip, store)
	// logger.Info("retrieved rows", "rows", _rows)
	if err != nil {
		return nil, err
	}
	if len(_rows) == 0 {
		return nil, gostore.ErrNotFound
	}
	return newSyncRows(_rows), nil
}

// AllCursor returns all entries in a store.
func (s *BoltStore) AllCursor(store string) (gostore.ObjectRows, error) {
	rows := common.NewCursorRows()
	go func(rows *common.CursorRows) {
		defer func() {
			rows.Done() <- true
		}()
		err := s.Db.View(func(tx *boltdb.Tx) error {
			nbkt := tx.Bucket([]byte(store))
			if nbkt == nil {
				return ErrNoNestedBuckets
			}
			c := nbkt.Cursor()
			//no skip needed. Get first item
			k, v := c.First()
			if k == nil {
				return gostore.ErrEOF
			}
			// listen to chan for next request
		OUTER:
			for {
				select {
				case <-rows.Exit():
					break OUTER
				case <-rows.NextChan():
					if k == nil {
						rows.OnNext(nil)
					} else {
						rows.OnNext([][]byte{k, v})
					}
					k, v = c.Next()
				}
			}
			return nil
		})
		if err != nil {
			logger.Error("cursor rows for "+store+" failed", "err", err.Error())
		}
	}(rows)
	return rows, nil
}

func (s *BoltStore) GetAll(count int, skip int, bucket string) (objs [][][]byte, err error) {

	err = s.Db.View(func(tx *boltdb.Tx) error {
		nbkt := tx.Bucket([]byte(bucket))
		if err != nil {
			return err
		}
		c := nbkt.Cursor()
		var skip_lim int = 1

		var lim int = 0
		//Skip a certain amount
		if skip > 0 {
			//make sure we hit the database once
			var target_count int = skip - 1
			for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Last()
			if k == nil {
				return err
			}
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				// logger.Info("count reached", "lim", lim, "count", count)
				return nil
			}
		}
		//Get next items after skipping or getting first item
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				// logger.Info("count reached", "lim", lim, "count", count)
				break
			}
		}
		return err
	})
	logger.Info("_GetAll done")
	return
}

func (s *BoltStore) _GetAllAfter(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *boltdb.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		var lim int = 0
		if skip > 0 {
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.Seek(key); k != nil; k, _ = c.Next() {
				logger.Info("Skipped ", string(k), "Current lim is ", skip_lim, " target count is ", target_count)
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Seek(key)
			if k != nil {
				objs = append(objs, [][]byte{k, v})
				lim++
			} else {
				return err
			}
			if lim == count {
				return nil
			}
		}
		for k, v := c.Next(); k != nil; k, v = c.Next() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				break
			}
		}
		return err
	})
	return
}

func (s *BoltStore) GetAllBefore(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *boltdb.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		var lim int = 0
		if skip > 0 {
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.Seek(key); k != nil; k, _ = c.Prev() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Seek(key)
			if k != nil {
				objs = append(objs, [][]byte{k, v})
				lim++
			} else {
				return err
			}
			if lim == count {
				return nil
			}
		}
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				break
			}
		}
		return err
	})
	return
}

func (s *BoltStore) _Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	b_prefix := []byte(prefix)
	err = s.Db.View(func(tx *boltdb.Tx) error {
		var lim int = 1
		c := tx.Bucket([]byte(resource)).Cursor()
		if skip > 0 {
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.Seek(b_prefix); k != nil; k, _ = c.Next() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Seek(b_prefix)
			if k != nil {
				objs = append(objs, [][]byte{k, v})
			} else {
				return err
			}
			if lim == count {
				return nil
			}
		}

		for k, v := c.Next(); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				break
			}
		}
		return nil
	})
	return
}

func (s *BoltStore) FilterSuffix(suffix []byte, count int, resource string) (objs [][]byte, err error) {
	s.CreateBucket(resource)
	b_prefix := []byte(suffix)
	err = s.Db.View(func(tx *boltdb.Tx) error {
		var lim int = 1
		c := tx.Bucket([]byte(resource)).Cursor()
		for k, v := c.Seek(b_prefix); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
			objs = append(objs, v)
			if lim == count {
				break
			}
			lim++
		}
		return nil
	})
	return
}

func (s *BoltStore) StreamFilter(key []byte, count int, resource string) chan []byte {

	s.CreateBucket(resource)
	//Uses channels to stream filtered keys
	ch := make(chan []byte)
	go func() {
		b_prefix := []byte(key)
		s.Db.View(func(tx *boltdb.Tx) error {
			var lim int = 1
			c := tx.Bucket([]byte(resource)).Cursor()
			for k, v := c.Seek(b_prefix); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
				ch <- v
				if lim == count {
					break
				}
				lim++
			}
			return nil
		})
		close(ch)
	}()
	return ch
}

func (s *BoltStore) StreamAll(count int, resource string) chan [][]byte {

	s.CreateBucket(resource)
	//Uses channels to stream filtered keys
	ch := make(chan [][]byte)
	go func() {
		s.Db.View(func(tx *boltdb.Tx) error {
			var lim int = 1
			c := tx.Bucket([]byte(resource)).Cursor()
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				ch <- [][]byte{k, v}
				if lim == count {
					break
				}
				lim++
			}
			close(ch)
			return nil
		})
	}()
	return ch
}

func (s *BoltStore) Stats(bucket string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})
	err = s.Db.View(func(tx *boltdb.Tx) error {
		v := tx.Bucket([]byte(bucket)).Stats()
		data["total_count"] = v.KeyN
		return nil
	})
	return
}

func (s *BoltStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}
func (s *BoltStore) Since(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	_rows, err := s._GetAllAfter([]byte(id), count, skip, store)
	if err != nil {
		return nil, err
	}
	return newSyncRows(_rows), nil
} //Get all recent items from a key
func (s *BoltStore) Before(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	_rows, err := s.GetAllBefore([]byte(id), count, skip, store)
	if err != nil {
		return nil, err
	}
	return newSyncRows(_rows), nil
} //Get all existing items before a key

func (s *BoltStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all recent items from a key
func (s *BoltStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all existing items before a key
func (s *BoltStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	return 0, gostore.ErrNotImplemented
} //Get all existing items before a key

func (s *BoltStore) Get(key string, store string, dst interface{}) error {
	data, err := s._Get(key, store)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}
func (s *BoltStore) SaveRaw(key string, val []byte, store string) error {
	if err := s._Save([]byte(key), val, store); err != nil {
		return err
	}
	return nil
}
func (s *BoltStore) Save(key, store string, src interface{}) (string, error) {
	data, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	err = s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(store))
		err := b.Put([]byte(key), data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{store, src})
	})
	return key, err
}
func (s *BoltStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, gostore.ErrNotImplemented
}
func (s *BoltStore) Update(key string, store string, src interface{}) error {
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

	err = s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(store))
		err := b.Put([]byte(key), data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{store, existing})
	})
	return err
}
func (s *BoltStore) Replace(key string, store string, src interface{}) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	err = s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(store))
		err := b.Put([]byte(key), data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{store, src})
	})
	return err
}
func (s *BoltStore) Delete(key string, store string) error {
	logger.Info("_Delete", "key", key, "bucket", store)
	err := s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(store))
		err := b.Delete([]byte(key))
		if err != nil {
			return err
		}
		return s.Indexer.UnIndexDocument(key)
	})
	return err
}

//Filter
func (s *BoltStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s *BoltStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s *BoltStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterGet", "filter", filter, "Store", store)

	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		var (
			data [][]byte
		)

		// res, err := s.Indexer.Query(s.getQueryString(store, filter))
		res, err := s.Indexer.QueryWithOptions(indexer.GetQueryString(store, query), 1, 0, false, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			return err
		}
		if res.Total == 0 {
			return gostore.ErrNotFound
		}
		// if res.Total > 1 {
		// 	return gostore.ErrDuplicatePk
		// }
		if bucket, err := s.getBucketList(store, filter); err == nil {
			data, err = s._NestedGet([]byte(res.Hits[0].ID), bucket)
		} else {
			data, err = s._Get(res.Hits[0].ID, store)
		}
		if err != nil {
			return err
		}

		err = json.Unmarshal(data[1], dst)
		return err
	}
	return gostore.ErrNotFound

}
func (s *BoltStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {

	if query, ok := filter["q"].(map[string]interface{}); ok {
		q := indexer.GetQueryString(store, query)
		logger.Info("FilterGetAll", "count", count, "skip", skip, "Store", store, "query", q)
		res, err := s.Indexer.QueryWithOptions(q, count, skip, false, []string{"*"}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Warn("err", "error", err)
			return nil, err
		}
		if res.Total == 0 {
			return nil, gostore.ErrNotFound
		}
		// logger.Debug("result", "result", res.Hits)
		// return NewIndexedSyncRows(store, res.Total, res, &s), nil
		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, nil
	}
	return nil, gostore.ErrNotFound
}

func (s *BoltStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, gostore.AggregateResult, error) {

	return nil, nil, gostore.ErrNotFound
}
func (s *BoltStore) FilterDelete(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterDelete", "filter", filter, "store", store)

	if query, ok := filter["q"].(map[string]interface{}); ok {
		res, err := s.Indexer.Query(indexer.GetQueryString(store, query))
		if err == nil {
			if res.Total == 0 {
				return gostore.ErrNotFound
			}
			// if res.Total > 1 {
			// 	return gostore.ErrDuplicatePk
			// }

			for _, v := range res.Hits {
				// err = s._Delete(v.ID, store)
				// if err != nil {
				// 	break
				// }
				// err = s.Indexer.UnIndexDocument(v.ID)
				// if err != nil {
				// 	break
				// }
				logger.Info("_Delete", "key", v.ID, "bucket", store)
				err = s.Db.Update(func(tx *boltdb.Tx) error {
					b := tx.Bucket([]byte(store))
					err := b.Delete([]byte(v.ID))
					if err != nil {
						return err
					}
					return s.Indexer.UnIndexDocument(v.ID)
				})
			}
		}
		return err
	}
	return gostore.ErrNotFound
}

func (s *BoltStore) FilterCount(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) (int64, error) {

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
func (s *BoltStore) GetByField(name, val, store string, dst interface{}) error { return nil }
func (s *BoltStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return gostore.ErrNotImplemented
}

func (s *BoltStore) BatchDelete(ids []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	return gostore.ErrNotImplemented
}

func (s *BoltStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s *BoltStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s *BoltStore) BatchInsert(data []interface{}, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(data))
	err = s.Db.Update(func(tx *boltdb.Tx) error {
		b := s.Indexer.BatchIndex()
		bkt := tx.Bucket([]byte(store))
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
			err = bkt.Put([]byte(key), data)
			if err != nil {
				return err
			}
			b.Index(key, IndexedData{store, src})
			// if err2 := s.Indexer.IndexDocument(key, IndexedData{store, src}); err2 != nil {
			// 	logger.Warn(err.Error())
			// 	return err2
			// }
			keys[i] = key
		}
		return s.Indexer.Batch(b)
	})
	return
}
func (s *BoltStore) BatchInsertKV(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	return nil, gostore.ErrNotImplemented
}
func (s *BoltStore) BatchInsertKVAndIndex(rows [][][]byte, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	return nil, gostore.ErrNotImplemented
}
func (s *BoltStore) Close() {
	if s.Db != nil {
		s.Db.Close()
	}
	if s.Indexer != nil {
		s.Indexer.Close()
	}
	logger.Debug("closing bolt")
}
