package firestoredb

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	gostoretesting "github.com/osiloke/gostore-contrib/testing"
)

var sa []byte

func init() {
	serviceAccountFile, ok := os.LookupEnv("GOOGLE_SERVICE_ACCOUNT")
	if ok {
		data, err := ioutil.ReadFile(serviceAccountFile)
		if err == nil {
			sa = data
		}
	}
}
func TestFirestoreStore_Get(t *testing.T) {
	ctx := context.Background()
	db := NewFirestoreStoreWithJSON(ctx, sa)
	if err := db.ClearStore("data"); err != nil {
		t.Error(err)
		return
	}
	gostoretesting.Test_Get(t, db)
}
func TestFirestoreStore_Query(t *testing.T) {
	ctx := context.Background()
	db := NewFirestoreStoreWithJSON(ctx, sa)
	if err := db.ClearStore("data"); err != nil {
		t.Error(err)
		return
	}
	gostoretesting.Test_Query(t, db)
}

func TestFirestoreStore_BatchInsert(t *testing.T) {
	ctx := context.Background()
	db := NewFirestoreStoreWithJSON(ctx, sa)
	if err := db.ClearStore("data"); err != nil {
		t.Error(err)
		return
	}
	gostoretesting.Test_BatchInsert(t, db)
}
