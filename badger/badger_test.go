package badger

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/search"
	"github.com/osiloke/gostore"
	"github.com/stretchr/testify/assert"
)

var rootPath = "./.testdata"

func init() {
	os.Mkdir(rootPath, 0777)
}

func createDB(name string) *BadgerStore {
	mode := int(0777)
	testDbPath := filepath.Join(rootPath, name)
	indexPath := filepath.Join(testDbPath, "/db.index")
	dbPath := filepath.Join(testDbPath, "/db")
	os.Mkdir(testDbPath, os.FileMode(mode))
	os.RemoveAll(dbPath)
	os.RemoveAll(indexPath)
	// os.Mkdir(dbPath, os.FileMode(mode))
	// ix, _ := indexer.NewMossIndexer(indexPath)
	_db, err := NewWithIndex(testDbPath, "")
	if err != nil {
		panic(err)
	}
	return _db
}
func removeDB(name string, db *BadgerStore) {
	if db != nil {
		db.Close()
	}
	os.RemoveAll(filepath.Join(rootPath, name))
}
func TestBadgerStore_Get(t *testing.T) {
	db := createDB("BatchInsert")
	defer removeDB("BatchInsert", db)
	store := "data"
	db.CreateTable(store, nil)
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
	db.BatchInsert(rows, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can retrieve",
			func(t *testing.T) {
				dst := map[string]interface{}{}
				row := rows[0].(map[string]interface{})
				db.Get(row["id"].(string), store, &dst)
				assert.Equal(t, row, dst, "retrieved row is not identical to saved row")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}
func TestBadgerStore_FilterGet(t *testing.T) {
	type args struct {
		filter map[string]interface{}
		store  string
		dst    interface{}
		opts   gostore.ObjectStoreOptions
	}

	db := createDB("filterGet")
	defer removeDB("filterGet", db)
	db.CreateTable("data", nil)

	key := gostore.NewObjectId().String()
	osi := map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"count": 10.0,
	}
	db.Save(key, "data", &osi)

	key2 := gostore.NewObjectId().String()
	tony := map[string]interface{}{
		"id":    key2,
		"name":  "tony emoekpere",
		"count": 11.0,
	}
	db.Save(key2, "data", &tony)

	var dst map[string]interface{}
	tests := []struct {
		name    string
		s       *BadgerStore
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "osiloke"}}, "data", &dst, nil},
			false,
		},
		{
			"not exist",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "unknown"}}, "data", &dst, nil},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.FilterGet(tt.args.filter, tt.args.store, tt.args.dst, tt.args.opts); (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.FilterGet() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(dst["id"], osi["id"]) {
				t.Errorf("BadgerStore.FilterGet() = %v, want %v", dst, osi)
			}
		})
	}
}
func TestBadgerStore_FilterGetAll(t *testing.T) {
	type args struct {
		filter map[string]interface{}
		count  int
		skip   int
		store  string
		opts   gostore.ObjectStoreOptions
	}

	db := createDB("filterGetAll")
	defer removeDB("filterGetAll", db)
	db.CreateTable("data", nil)
	key := gostore.NewObjectId().String()
	db.Save(key, "data", &map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"count": 10.0,
	})
	key2 := gostore.NewObjectId().String()
	db.Save(key2, "data", &map[string]interface{}{
		"id":    key2,
		"name":  "emike emoekpere",
		"count": 10.0,
	})
	key3 := gostore.NewObjectId().String()
	db.Save(key3, "data", &map[string]interface{}{
		"id":    key3,
		"name":  "oduffa emoekpere",
		"count": 11.0,
	})
	key4 := gostore.NewObjectId().String()
	db.Save(key4, "data", &map[string]interface{}{
		"id":    key4,
		"name":  "tony emoekpere",
		"count": 11.0,
	})
	tests := []struct {
		name string
		s    *BadgerStore
		args args
		// want    gostore.ObjectRows
		wantErr bool
	}{
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "*emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "emike emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "tony emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "oduffa emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "osiloke emoekpere"}}, 10, 0, "data", nil},
			false,
		},
	}
	tt := tests[0]
	got, err := tt.s.FilterGetAll(tt.args.filter, tt.args.count, tt.args.skip, tt.args.store, tt.args.opts)
	if (err != nil) != tt.wantErr {
		t.Errorf("BadgerStore.FilterGetAll() error = %v, wantErr %v", err, tt.wantErr)
		return
	}
	for _, tt := range tests[1:] {
		t.Run(tt.name, func(t *testing.T) {

			var dst map[string]interface{}
			ok, _ := got.Next(&dst)
			if ok != !tt.wantErr {
				t.Errorf("BadgerStore.FilterGetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// if !reflect.DeepEqual(got, tt.want) {
			// 	t.Errorf("BadgerStore.FilterGetAll() = %v, want %v", got, tt.want)
			// }
		})
	}
}
func TestBadgerStore_Query(t *testing.T) {
	type args struct {
		filter     map[string]interface{}
		aggregates map[string]interface{}
		count      int
		skip       int
		store      string
		opts       gostore.ObjectStoreOptions
	}

	db := createDB("Query")
	defer removeDB("Query", db)
	db.CreateTable("data", nil)
	key := gostore.NewObjectId().String()
	db.Save(key, "data", &map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"type":  "person",
		"count": "12",
	})
	key2 := gostore.NewObjectId().String()
	db.Save(key2, "data", &map[string]interface{}{
		"id":    key2,
		"name":  "emike emoekpere",
		"type":  "person",
		"count": "10",
	})
	key3 := gostore.NewObjectId().String()
	db.Save(key3, "data", &map[string]interface{}{
		"id":    key3,
		"name":  "oduffa emoekpere",
		"type":  "person",
		"count": "11",
	})
	key4 := gostore.NewObjectId().String()
	db.Save(key4, "data", &map[string]interface{}{
		"id":    key4,
		"name":  "tony emoekpere",
		"type":  "person",
		"count": "11",
	})
	tests := []struct {
		name string
		s    *BadgerStore
		args args
		// want    gostore.ObjectRows
		wantErr bool
	}{
		{
			"get item",
			db,
			args{
				map[string]interface{}{"type": "person"},
				map[string]interface{}{
					"top": map[string]interface{}{
						"count": map[string]interface{}{
							"name":  "topCount",
							"field": "count",
							"count": 2,
						},
					},
				},
				10,
				0,
				"data",
				nil,
			},
			false,
		},
	}
	tt := tests[0]
	rows, agg, err := tt.s.Query(tt.args.filter, tt.args.aggregates, tt.args.count, tt.args.skip, tt.args.store, tt.args.opts)
	if (err != nil) != tt.wantErr {
		t.Errorf("BadgerStore.Query() error = %v, wantErr %v", err, tt.wantErr)
		return
	}
	logger.Debug("facets", "facets", agg)
	assert.NotNil(t, rows, "rows were empty")
	assert.Equal(t, gostore.AggregateResult{
		"topCount": gostore.Match{
			Field:       "count",
			UnMatched:   1,
			Matched:     4,
			Missing:     0,
			NumberRange: search.NumericRangeFacets(nil),
			DateRange:   search.DateRangeFacets(nil),
			Top:         search.TermFacets{{"11", 2}, {"10", 1}},
		},
	}, agg, "aggregate data was not retrieved")
}
func TestBadgerStore_BatchInsert(t *testing.T) {
	db := createDB("BatchInsert")
	defer removeDB("BatchInsert", db)
	store := "data"
	db.CreateTable(store, nil)
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
	keys, err := db.BatchInsert(rows, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"No Errors",
			func(t *testing.T) {
				assert.Nil(t, err, "errors while batch inserting")
			},
		},
		{
			"Accurate keys returned",
			func(t *testing.T) {
				assert.Equal(t, len(keys), len(rows), "inconsistency with returned keys count")
			},
		},
		{
			"Query for all keys should match batch insert",
			func(t *testing.T) {
				storedRows, err := db.All(10, 0, store)
				assert.Nil(t, err, "errors while retrieving all entries")

				ix := 0
				for {
					_, ok := storedRows.NextRaw()
					if !ok {
						break
					}
					ix++
				}
				assert.Equal(t, 4, ix, "stored rows inconsistency")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}

func TestBadgerStore_SaveTX(t *testing.T) {
	db := createDB("SaveTX")
	defer removeDB("SaveTX", db)
	store := "data"
	db.CreateTable(store, nil)
	rows := []map[string]interface{}{
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
	type args struct {
		key   string
		store string
		src   interface{}
		txn   gostore.Transaction
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"saveOne",
			args{
				rows[0]["id"].(string),
				"data",
				rows[0],
				db.UpdateTransaction(),
			},
			rows[0]["id"].(string),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.SaveTX(tt.args.key, tt.args.store, tt.args.src, tt.args.txn)
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			d := make(map[string]interface{})
			err = db.GetTX(tt.args.key, tt.args.store, &d, tt.args.txn)
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Log(d)
			if (d["id"].(string) != tt.args.key) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", errors.New("item not saved in transaction"), tt.wantErr)
				return
			}
			err = tt.args.txn.Commit()
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("BadgerStore.SaveTX() = %v, want %v", got, tt.want)
			}
		})
	}
}
