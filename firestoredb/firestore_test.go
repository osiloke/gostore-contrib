package firestoredb

import (
	"context"
	"testing"

	gostoretesting "github.com/osiloke/gostore-contrib/testing"
)

func TestFirestoreStore_Get(t *testing.T) {
	ctx := context.Background()
	db := NewFirestoreStoreWithServiceAccount(ctx, "./testServiceAccount.json")
	gostoretesting.Test_Get(t, db)
}
