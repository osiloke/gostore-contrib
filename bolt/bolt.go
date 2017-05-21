package bolt

//TODO: Extract methods into functions
import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	boltdb "github.com/boltdb/bolt"
	"github.com/dustin/gojson"
	"github.com/mgutz/logxi/v1"
	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/indexer"
)

var logger = log.New("gostore-contrib.bolt")

type HasID interface {
	GetId() string
}

type TableConfig struct {
	NestedBucketFieldMatcher map[string]*regexp.Regexp //defines fields to be used to extract nested buckets for data
}

type BoltStore struct {
	Bucket      []byte
	Db          *boltdb.DB
	Indexer     *indexer.Indexer
	tableConfig map[string]*TableConfig
}

type IndexedData struct {
	Bucket string      `json:"bucket"`
	Data   interface{} `json:"data"`
}

func (d *IndexedData) Type() string {
	return "indexed_data"
}

func New(path string) (s gostore.ObjectStore, err error) {
	db, err := boltdb.Open(path, 0600, nil)
	if err != nil {
		return
	}
	indexMapping := bleve.NewIndexMapping()
	// bucketMapping := bleve.NewDocumentMapping()
	// dataMapping := bleve.NewDocumentMapping()
	// bucketMapping.AddSubDocumentMapping("data", dataMapping)
	// indexMapping.AddDocumentMapping("indexed_data", bucketMapping)
	indexPath := path + ".index"
	index := indexer.NewIndexer(indexPath, indexMapping)
	s = BoltStore{[]byte("_default"), db, index, make(map[string]*TableConfig)}
	//	e.CreateBucket(bucket)
	return
}

func (s BoltStore) CreateDatabase() error {
	return nil
}

func (s BoltStore) CreateTable(table string, config interface{}) error {
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

func (s BoltStore) GetStore() interface{} {
	return s.Db
}

func (s BoltStore) CreateBucket(bucket string) error {
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

func (s BoltStore) getBucketList(store string, data map[string]interface{}) (bucket []string, err error) {
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

func (s BoltStore) _Get(key, resource string) (v [][]byte, err error) {
	logger.Info("_Get", "key", key, "bucket", resource)
	_key := []byte(key)
	vv, err := Get(_key, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{_key, vv}
	}
	return
}

func (s BoltStore) _PrefixGet(prefix []byte, resource string) (v [][]byte, err error) {
	kk, vv, err := PrefixGet(prefix, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{kk, vv}
	}
	return
}

func (s BoltStore) _Save(key []byte, data []byte, resource string) error {
	err := s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Put(key, data)
		return err
	})
	return err
}
func (s BoltStore) _SaveTx(key []byte, data []byte, resource string) func(tx *boltdb.Tx) error {
	return func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Put(key, data)
		return err
	}
}

func (s BoltStore) _Delete(key string, resource string) error {
	logger.Info("_Delete", "key", key, "bucket", resource)
	err := s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete([]byte(key))
		return err
	})
	return err
}

func (s BoltStore) DeleteAll(resource string) error {
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

func (s BoltStore) _NestedGet(key []byte, bucket []string) (v [][]byte, err error) {
	_key := []byte(key)
	vv, err := NestedGet(_key, bucket, s.Db)
	if vv != nil {
		v = [][]byte{_key, vv}
	}
	return
}
func (s BoltStore) _NestedSave(key []byte, data []byte, bucket []string) error {
	err := s.Db.Update(func(tx *boltdb.Tx) error {
		nbkt, err := getBucket(tx, bucket)
		if err == nil {
			err = nbkt.Put(key, data)
		}
		return err
	})
	return err
}
func (s BoltStore) _NestedDelete(key string, resource string) error {
	err := s.Db.Update(func(tx *boltdb.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete([]byte(key))
		return err
	})
	return err
}

func newBoltRows(rows [][][]byte) BoltRows {
	total := len(rows)
	closed := make(chan bool)
	retrieved := make(chan string)
	nextItem := make(chan interface{})
	ci := 0
	b := BoltRows{nextItem: nextItem, closed: closed, retrieved: retrieved}
	println(rows)
	go func() {
	OUTER:
		for {
			select {
			case <-closed:
				logger.Info("newBoltRows closed")
				break OUTER
				return
			case item := <-nextItem:
				// logger.Info("current index", "ci", ci, "total", total)
				if ci == total {
					b.lastError = gostore.ErrEOF
					// logger.Info("break bolt rows loop")
					break OUTER
					return
				} else {
					current := rows[ci]
					if err := json.Unmarshal(current[1], item); err != nil {
						logger.Warn(err.Error())
						b.lastError = err
						retrieved <- ""
						break OUTER
						return
					} else {
						retrieved <- string(current[0])
						ci++
					}
				}
			}
		}
		b.Close()
	}()
	return b
}

//New Api
type BoltRows struct {
	rows      [][][]byte
	i         int
	length    int
	retrieved chan string
	closed    chan bool
	nextItem  chan interface{}
	lastError error
	isClosed  bool
	sync.RWMutex
}

func (s BoltRows) Next(dst interface{}) (bool, error) {
	if s.lastError != nil {
		return false, s.lastError
	}
	//NOTE: Consider saving id in bolt data
	var _dst map[string]interface{}
	s.nextItem <- &_dst
	key := <-s.retrieved
	if key == "" {
		return false, nil
	}
	_dst["id"] = key
	_data, _ := json.Marshal(&_dst)
	json.Unmarshal(_data, dst)
	return true, nil
}

func (s BoltRows) NextRaw() ([]byte, bool) {
	return nil, false
}
func (s BoltRows) LastError() error {
	return s.lastError
}
func (s BoltRows) Close() {
	// s.rows = nil
	// s.closed <- true
	logger.Info("close bolt rows")
	close(s.closed)
	close(s.retrieved)
	close(s.nextItem)
	// s.isClosed = true
}

func (s BoltStore) All(count int, skip int, store string) (gostore.ObjectRows, error) {
	s.CreateBucket(store)
	_rows, err := s.GetAll(count, skip, []string{store})
	// logger.Info("retrieved rows", "rows", _rows)
	if err != nil {
		return nil, err
	}
	return newBoltRows(_rows), nil
}

func (s BoltStore) GetAll(count int, skip int, bucket []string) (objs [][][]byte, err error) {

	err = s.Db.View(func(tx *boltdb.Tx) error {
		nbkt, err := getBucket(tx, bucket)
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

func (s BoltStore) _GetAllAfter(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
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

func (s BoltStore) GetAllBefore(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
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

func (s BoltStore) _Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
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

func (s BoltStore) FilterSuffix(suffix []byte, count int, resource string) (objs [][]byte, err error) {
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

func (s BoltStore) StreamFilter(key []byte, count int, resource string) chan []byte {

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

func (s BoltStore) StreamAll(count int, resource string) chan [][]byte {

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

func (s BoltStore) Stats(bucket string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})
	err = s.Db.View(func(tx *boltdb.Tx) error {
		v := tx.Bucket([]byte(bucket)).Stats()
		data["total_count"] = v.KeyN
		return nil
	})
	return
}

func (s BoltStore) AllCursor(store string) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

func (s BoltStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}
func (s BoltStore) Since(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	_rows, err := s._GetAllAfter([]byte(id), count, skip, store)
	if err != nil {
		return nil, err
	}
	return newBoltRows(_rows), nil
} //Get all recent items from a key
func (s BoltStore) Before(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	_rows, err := s.GetAllBefore([]byte(id), count, skip, store)
	if err != nil {
		return nil, err
	}
	return newBoltRows(_rows), nil
} //Get all existing items before a key

func (s BoltStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all recent items from a key
func (s BoltStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
} //Get all existing items before a key
func (s BoltStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	return 0, gostore.ErrNotImplemented
} //Get all existing items before a key

func (s BoltStore) Get(key string, store string, dst interface{}) error {
	data, err := s._Get(key, store)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}
func (s BoltStore) SaveRaw(key string, val []byte, store string) error {
	if err := s._Save([]byte(key), val, store); err != nil {
		return err
	}
	return nil
}
func (s BoltStore) Save(store string, src interface{}) (string, error) {
	var key string
	// var nestedRe *regexp.Regexp
	// var nestedKeyVal string
	if _v, ok := src.(map[string]interface{}); ok {
		if k, ok := _v["id"].(string); ok {
			key = k
		} else {
			key = gostore.NewObjectId().String()
			_v["id"] = key
		}
		// for nestedField, re := range s.tableConfig[store].NestedBucketFieldMatcher {
		// 	if nestedFieldValue, ok := _v[nestedField]; ok {
		// 		nestedRe = re
		// 		nestedKeyVal = nestedFieldValue.(string)
		// 		break
		// 	}
		// }
	} else if _v, ok := src.(HasID); ok {
		key = _v.GetId()
	} else {
		// if _key, err := shortid.Generate(); err == nil {
		// 	key = _key
		// } else {
		// 	logger.Error(ErrKeyNotValid.Error(), "err", err)
		// 	return ErrKeyNotValid
		// }
		key = gostore.NewObjectId().String()
	}
	data, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	// if nestedRe != nil {
	// 	bucket := []string{store}
	// 	for _, match := range nestedRe.FindAllString(nestedKeyVal, -1) {
	// 		bucket = append(bucket, match)
	// 	}
	// 	if err := s._NestedSave([]byte(key), data, bucket); err != nil {
	// 		return "", err
	// 	}

	// } else {
	if err := s._Save([]byte(key), data, store); err != nil {
		return "", err
	}
	// }
	err = s.Indexer.IndexDocument(key, IndexedData{store, src})
	return key, err
}
func (s BoltStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, gostore.ErrNotImplemented
}
func (s BoltStore) Update(key string, store string, src interface{}) error {
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
	if err := s._Save([]byte(key), data, store); err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(key, IndexedData{store, existing})
	return err
}
func (s BoltStore) Replace(key string, store string, src interface{}) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	if err := s._Save([]byte(key), data, store); err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(key, IndexedData{store, src})
	return err
}
func (s BoltStore) Delete(key string, store string) error {
	return s._Delete(key, store)
}

//Filter
func (s BoltStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s BoltStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}
func (s BoltStore) getQueryString(store string, filter map[string]interface{}) string {
	queryString := "+bucket:" + store
	for k, v := range filter {
		if _v, ok := v.(int); ok {
			queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, _v)
			queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, _v)
		} else if v == "" {
		} else {
			val_rune := []rune(v.(string))
			first := string(val_rune[0])
			if first == "<" {
				v = string(val_rune[1:])
				queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, v)
			} else if first == ">" {
				v = string(val_rune[1:])
				queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, v)

			} else {
				prefix := "+"
				if first == "!" {
					prefix = "-"
					v = string(val_rune[1:])
				}
				queryString = fmt.Sprintf(`%s %sdata.%s:"%v"`, queryString, prefix, k, v)
			}
		}
	}
	return strings.Replace(queryString, "\"", "", -1)
}
func (s BoltStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterGet", "filter", filter, "Store", store)
	//check if filter contains a nested field which is used to traverse a sub bucket
	var (
		data [][]byte
	)

	// res, err := s.Indexer.Query(s.getQueryString(store, filter))
	res, err := s.Indexer.QueryWithOptions(s.getQueryString(store, filter), 1, 0, false, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
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
func (s BoltStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	q := s.getQueryString(store, filter)
	logger.Info("FilterGetAll", "count", count, "skip", skip, "Store", store, "query", q)
	res, err := s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
	if err != nil {
		logger.Warn("err", "error", err)
		return nil, err
	}
	if res.Total == 0 {
		return nil, gostore.ErrNotFound
	}
	return NewIndexedBoltRows(store, res.Total, res, &s), nil
}

func (s BoltStore) FilterDelete(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	logger.Info("FilterDelete", "filter", filter, "store", store)
	res, err := s.Indexer.Query(s.getQueryString(store, filter))
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

func (s BoltStore) FilterCount(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	res, err := s.Indexer.Query(s.getQueryString(store, filter))
	if err != nil {
		return 0, err
	}
	if res.Total == 0 {
		return 0, gostore.ErrNotFound
	}
	return int64(res.Total), nil
}

//Misc gets
func (s BoltStore) GetByField(name, val, store string, dst interface{}) error { return nil }
func (s BoltStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return gostore.ErrNotImplemented
}

func (s BoltStore) BatchDelete(ids []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	return gostore.ErrNotImplemented
}

func (s BoltStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s BoltStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	return gostore.ErrNotImplemented
}

func (s BoltStore) BatchInsert(data []interface{}, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(data))
	errCnt := 0
	var wg sync.WaitGroup
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
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err2 := s.Db.Batch(s._SaveTx([]byte(key), data, store)); err2 != nil {
				errCnt += 1
				logger.Warn(err.Error())
				return
			}
			if err2 := s.Indexer.IndexDocument(key, IndexedData{store, src}); err2 != nil {
				errCnt += 1
				logger.Warn(err.Error())
			}
			keys = append(keys, key)
		}()
	}
	wg.Wait()
	if errCnt > 0 {
		return nil, errors.New("failed batch insert")
	}
	return
}
func (s BoltStore) Close() {}
