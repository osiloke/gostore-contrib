package badger

import (

	// "io/ioutil"

	"testing"

	"os"

	"github.com/osiloke/gostore"
	. "github.com/smartystreets/goconvey/convey"
)

func createDB(path string) gostore.ObjectStore {
	path = "./test/" + path
	dbPath := path + "/db"
	mode := int(0777)
	os.RemoveAll(dbPath)
	os.RemoveAll(path + ".index")
	os.Mkdir(dbPath, os.FileMode(mode))
	_db, _ := New(path)
	return _db
}
func removeDB(path string, db gostore.ObjectStore) {
	db.Close()
	path = "./test/" + path
	os.RemoveAll(path + "/db")
	os.RemoveAll(path + ".index")
}
func TestFilterGet(t *testing.T) {
	db := createDB("filterGet")
	defer removeDB("filterGet", db)
	db.CreateTable("data", nil)
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
			db.Save("data", &data)

			key2 := gostore.NewObjectId().String()
			data2 := map[string]interface{}{
				"id":    key2,
				"name":  "tony emoekpere",
				"count": 11,
			}
			db.Save("data", &data2)

			Convey("Then filtering should return the object", func() {
				var dst map[string]interface{}
				err := db.FilterGet(map[string]interface{}{"name": "osiloke"},
					"data", &dst, nil)
				if err != nil {
					Println(err)
					So(err, ShouldEqual, nil)
				} else {
					So(dst["id"].(string), ShouldEqual, key)
				}
			})
		})
	})
}

func TestFilterGetAll(t *testing.T) {
	db := createDB("filterGetAll")
	defer removeDB("filterGetAll", db)
	db.CreateTable("data", nil)
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

			Convey("Then filtering should return two rows", func() {
				rows, err := db.FilterGetAll(map[string]interface{}{"count": 11},
					10, 0, "data", nil)
				defer rows.Close()
				if err != nil {
					Println(err)
					So(err, ShouldEqual, nil)
				} else {
					count := 0
					for {
						var dst interface{}
						ok, err := rows.Next(&dst)
						if err != nil {
							Println(err)
							break
						}
						if !ok {
							break
						}
						Println(dst)
						count++
					}
					So(count, ShouldEqual, 2)
				}

			})
		})
	})
}

func TestFilterGetAllNoResults(t *testing.T) {
	db := createDB("filterGetAllNoResults")
	defer removeDB("filterGetAllNoResults", db)
	db.CreateTable("data", nil)
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

			Convey("Then filtering for non existent rows should return ErrNotFound", func() {
				_, err := db.FilterGetAll(map[string]interface{}{"count": 12},
					10, 0, "data", nil)
				So(err, ShouldEqual, gostore.ErrNotFound)

			})
		})
	})
}
