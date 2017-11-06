package bolt_test

import (
	"fmt"
	"github.com/osiloke/gostore"
	. "github.com/osiloke/gostore-contrib/bolt"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

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

func getDB(boltPath, indexPath string) *BoltStore {
	DB, err := NewWithPaths(boltPath, indexPath)
	if err != nil {
		panic(err)
	}
	return DB

}
func TestSave(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	DB.CreateTable("data", nil)
	// Only pass t into top-level Convey calls
	Convey("Given a map to be saved", t, func() {

		Convey("Then saving the map", func() {
			key := gostore.NewObjectId().String()
			data := map[string]interface{}{
				"id":    key,
				"name":  "osiloke emoekpere",
				"count": 10,
			}
			newKey, err := DB.Save(key, "data", &data)

			Convey("Should give no error", func() {
				if err != nil {
					So(err, ShouldEqual, nil)
				} else {
					So(newKey, ShouldEqual, key)
				}
			})
		})
	})
}
func TestDelete(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	DB.CreateTable("data", nil)
	// Only pass t into top-level Convey calls
	Convey("Given a map to be saved", t, func() {

		Convey("Then saving the map", func() {
			key := gostore.NewObjectId().String()
			data := map[string]interface{}{
				"id":    key,
				"name":  "osiloke emoekpere",
				"count": 10,
			}
			newKey, _ := DB.Save(key, "data", &data)

			Convey("Deleting should give no error", func() {
				err := DB.Delete(key, "data")
				if err != nil {
					So(err, ShouldEqual, nil)
				} else {
					So(newKey, ShouldEqual, key)
				}
				Convey("Getting a list of keys should give error", func() {
					var d interface{}
					err := DB.Get(key, "data", &d)
					So(err, ShouldNotEqual, nil)
					So(err, ShouldEqual, gostore.ErrNotFound)
				})
			})
		})
	})
}
func TestGet(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	DB.CreateTable("data", nil)
	// Only pass t into top-level Convey calls
	Convey("Given a map to be saved", t, func() {

		Convey("Then saving the map", func() {
			key := gostore.NewObjectId().String()
			data := map[string]interface{}{
				"id":    key,
				"name":  "osiloke emoekpere",
				"count": 10,
			}
			_, err := DB.Save(key, "data", &data)
			if err != nil {
				panic(err)
			}

			Convey("Then filtering should return the object", func() {
				var dst map[string]interface{}
				err = DB.Get(key, "data", &dst)
				if err != nil {
					So(err, ShouldEqual, nil)
				} else {
					So(dst["id"].(string), ShouldEqual, key)
				}
			})
		})
	})
}
func TestFilterGet(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	DB.CreateTable("data", nil)
	// Only pass t into top-level Convey calls
	Convey("Given a map to be saved", t, func() {
		// data := struct {
		// 	Name  string `json:"name"`
		// 	Count int    `json:"count"`
		// }{
		// 	Name:  "osiloke emoekpere",
		// 	Count: 10,
		// }

		Convey("Then saving the map", func() {
			key := gostore.NewObjectId().String()
			data := map[string]interface{}{
				"id":    key,
				"name":  "osiloke emoekpere",
				"count": 10,
			}
			DB.Save(key, "data", &data)

			key2 := gostore.NewObjectId().String()
			data2 := map[string]interface{}{
				"id":    key2,
				"name":  "tony emoekpere",
				"count": 11,
			}
			DB.Save(key2, "data", &data2)

			Convey("Then filtering should return the object", func() {
				var dst map[string]interface{}
				err := DB.FilterGet(map[string]interface{}{"name": "osiloke"},
					"data", &dst, nil)
				if err != nil {
					So(err, ShouldEqual, nil)
				} else {
					So(dst["id"].(string), ShouldEqual, key)
				}
			})
		})
	})
}

func TestFilterGetAll(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	DB.CreateTable("data", nil)
	// Only pass t into top-level Convey calls
	Convey("Given a map to be saved", t, func() {
		Convey("Then saving the map", func() {

			data := []map[string]interface{}{
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
			for _, d := range data {
				DB.Save(d["id"].(string), "data", d)
			}

			Convey("Then filtering should return two rows", func() {
				rows, err := DB.FilterGetAll(map[string]interface{}{"count": 11},
					10, 0, "data", nil)
				defer rows.Close()
				if err != nil {
					So(err, ShouldEqual, nil)
				} else {
					count := 0
					for {
						var dst interface{}
						ok, err := rows.Next(&dst)
						if err != nil {
							break
						}
						if !ok {
							break
						}
						count++
					}
					So(count, ShouldEqual, 2)
				}

			})
		})
	})
}

func TestFilterGetAllNoResults(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	DB.CreateTable("data", nil)
	// Only pass t into top-level Convey calls
	Convey("Given a map to be saved", t, func() {
		Convey("Then saving the map", func() {
			data := []map[string]interface{}{
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
			for _, d := range data {
				DB.Save(d["id"].(string), "data", d)
			}

			Convey("Then filtering for non existent rows should return ErrNotFound", func() {
				_, err := DB.FilterGetAll(map[string]interface{}{"count": 12},
					10, 0, "data", nil)
				So(err, ShouldEqual, gostore.ErrNotFound)

			})
		})
	})
}
func TestBatchInsert(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	store := "data"
	DB.CreateTable(store, nil)
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
	keys, err := DB.BatchInsert(rows, store, nil)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}

func TestAll(t *testing.T) {
	boltPath := tempPath()
	indexPath := tempPath()
	DB := getDB(boltPath, indexPath)
	defer func() {
		DB.Close()
		os.Remove(boltPath)
		os.Remove(indexPath)
	}()
	store := "data"
	DB.CreateTable(store, nil)
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
	keys, err := DB.BatchInsert(rows, store, nil)
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
				storedRows, err := DB.All(10, 0, store)
				assert.Nil(t, err, "errors while retrieving all entries")

				count := 0
				for {
					_, ok := storedRows.NextRaw()
					if !ok {
						break
					}
					count++
				}
				assert.Equal(t, len(rows), count, "stored rows inconsistency")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}
