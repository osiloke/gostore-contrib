package testing

import (
	"testing"

	"github.com/osiloke/gostore"
	"github.com/stretchr/testify/assert"
)

// Test_Get test if a gostore can retrieve a singlee item
func Test_Get(t *testing.T, db gostore.ObjectStore) {
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

// Test_BatchInsert test if a gostore can insert multiple entries
func Test_BatchInsert(t *testing.T, db gostore.ObjectStore) {
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
				res, _ := db.All(10, 0, store)
				total := 0
				ok := true
				for ok {
					_, ok = res.NextRaw()
					if ok {
						total++
					}
				}
				assert.Equal(t, total, len(rows), "number of rows saved is equal to number retrieved")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}
