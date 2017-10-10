package badger

import (
	"os"
	"reflect"
	"testing"

	"github.com/osiloke/gostore"
	"github.com/stretchr/testify/assert"
)

func createDB(path string) BadgerStore {
	mode := int(0777)
	path = "./ddd/" + path
	os.Mkdir(path, os.FileMode(mode))
	dbPath := path + "/db"
	os.RemoveAll(dbPath)
	os.RemoveAll(path + ".index")
	os.Mkdir(dbPath, os.FileMode(mode))
	_db, err := New(path)
	if err != nil {
		panic(err)
	}
	return _db.(BadgerStore)
}
func removeDB(path string, db gostore.ObjectStore) {
	if db != nil {
		db.Close()
	}
	path = "./test/" + path
	os.RemoveAll(path + "/db")
	os.RemoveAll(path + ".index")
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
			"count": 10,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11,
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
				assert.Equal(t, dst, row, "retrieved row is not identical to saved row")
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
		"count": 10,
	}
	db.Save("data", &osi)

	key2 := gostore.NewObjectId().String()
	tony := map[string]interface{}{
		"id":    key2,
		"name":  "tony emoekpere",
		"count": 11,
	}
	db.Save("data", &tony)

	var dst map[string]interface{}
	tests := []struct {
		name    string
		s       BadgerStore
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			"get item",
			db,
			args{map[string]interface{}{"name": "osiloke"}, "data", &dst, nil},
			false,
		},
		{
			"not exist",
			db,
			args{map[string]interface{}{"name": "unknown"}, "data", &dst, nil},
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
	db.Save("data", &map[string]interface{}{
		"id":    gostore.NewObjectId().String(),
		"name":  "osiloke emoekpere",
		"count": 10,
	})
	db.Save("data", &map[string]interface{}{
		"id":    gostore.NewObjectId().String(),
		"name":  "emike emoekpere",
		"count": 10,
	})
	db.Save("data", &map[string]interface{}{
		"id":    gostore.NewObjectId().String(),
		"name":  "oduffa emoekpere",
		"count": 11,
	})
	db.Save("data", &map[string]interface{}{
		"id":    gostore.NewObjectId().String(),
		"name":  "tony emoekpere",
		"count": 11,
	})
	tests := []struct {
		name string
		s    BadgerStore
		args args
		// want    gostore.ObjectRows
		wantErr bool
	}{
		{
			"get item",
			db,
			args{map[string]interface{}{"name": "*emoekpere"}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"name": "emike emoekpere"}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"name": "tony emoekpere"}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"name": "oduffa emoekpere"}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"name": "osiloke emoekpere"}, 10, 0, "data", nil},
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
func TestBadgerStore_BatchInsert(t *testing.T) {
	db := createDB("BatchInsert")
	defer removeDB("BatchInsert", db)
	store := "data"
	db.CreateTable(store, nil)
	rows := []interface{}{
		map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11,
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

				count := 1
				for {
					_, ok := storedRows.NextRaw()
					if !ok {
						break
					}
					count++
				}
				assert.Equal(t, 4, count, "stored rows inconsistency")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}
