package firestoredb

import (
	"context"
	"testing"

	gostoretesting "github.com/osiloke/gostore-contrib/testing"
)

func TestFirestoreStore_Get(t *testing.T) {
	ctx := context.Background()
	db := NewFirestoreStoreWithServiceAccount(ctx, "./testServiceAccount.json")
	if err := db.ClearStore("data"); err != nil {
		t.Error(err)
		return
	}
	gostoretesting.Test_Get(t, db)
}

func TestFirestoreStore_BatchInsert(t *testing.T) {
	ctx := context.Background()
	db := NewFirestoreStoreWithServiceAccount(ctx, "./testServiceAccount.json")
	if err := db.ClearStore("data"); err != nil {
		t.Error(err)
		return
	}
	gostoretesting.Test_BatchInsert(t, db)
}
