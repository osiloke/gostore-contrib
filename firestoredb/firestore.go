package firestoredb

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/common"
	"github.com/osiloke/gostore-contrib/indexer"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var logger = common.Logger("firestoredb")

type Firestore struct {
	ctx     context.Context
	fs      *firestore.Client
	Indexer indexer.Indexer
}

// New*FirestoreStore create a new firestore
func NewFirestoreStore(ctx context.Context, projectID string) *Firestore {
	fs, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}
	s := Firestore{fs: fs, ctx: ctx}

	return &s
}

func NewFirestoreStoreWithServiceAccount(ctx context.Context, serviceAccountFile string) *Firestore {
	sa := option.WithCredentialsFile(serviceAccountFile)
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		log.Fatalln(err)
	}
	fs, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	return &Firestore{fs: fs, ctx: ctx}
}

func NewFirestoreStoreWithJSON(ctx context.Context, data []byte) *Firestore {
	sa := option.WithCredentialsJSON(data)
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		log.Fatalln(err)
	}
	fs, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	return &Firestore{fs: fs, ctx: ctx}
}

func (s *Firestore) keyForTable(table string) string {
	return "t$" + table
}

func (s *Firestore) keyForTableId(table, id string) string {
	return s.keyForTable(table) + "|" + id
}

// CreateDatabase this should create a new collection
func (k *Firestore) CreateDatabase() (err error) {

	return nil
}

// CreateTable creates a firestore subcollection in a database
func (k *Firestore) CreateTable(table string, sample interface{}) error {
	return nil
}

func (k *Firestore) ClearStore(store string) error {
	client := k.fs
	batchSize := 500
	ref := client.Collection(store)
	for {
		// Get a batch of documents
		iter := ref.Limit(batchSize).Documents(k.ctx)
		numDeleted := 0

		// Iterate through the documents, adding
		// a delete operation for each one to a
		// WriteBatch.
		batch := client.Batch()
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			batch.Delete(doc.Ref)
			numDeleted++
		}

		// If there are no documents to delete,
		// the process is over.
		if numDeleted == 0 {
			return nil
		}

		_, err := batch.Commit(k.ctx)
		if err != nil {
			return err
		}
	}
}

// GetStore return firestore client
func (k *Firestore) GetStore() interface{} {
	return k.fs
}

// Stats return some stats about the firestore database
func (k *Firestore) Stats(store string) (map[string]interface{}, error) {
	return nil, gostore.ErrNotImplemented
}

// All return all rows in a store
func (k *Firestore) All(count int, skip int, store string) (gostore.ObjectRows, error) {
	iter := k.fs.Collection(store).Limit(count).Offset(skip).Documents(k.ctx)
	return &TransactionRows{iter, nil}, nil
}

// AllCursor returns a cursor for listing all entries in a collection
func (k *Firestore) AllCursor(store string) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

// AllWithinRange returns all entries in a collection within a range
func (k *Firestore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {

	return nil, gostore.ErrNotImplemented
}

// Since returns rows greater than a specific row
func (k *Firestore) Since(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	return nil, gostore.ErrNotImplemented
}

func (k *Firestore) Before(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	// rows, err := SeekBefore(k.store, id, count, skip, store)
	return nil, errors.New("not implemented")
}

func (k *Firestore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k *Firestore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k *Firestore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	panic("not implemented")
}

// Get a key from a collection
func (k *Firestore) Get(key string, store string, dst interface{}) error {
	snap, err := k.fs.Collection(store).Doc(key).Get(k.ctx)
	if err != nil {
		return err
	}
	return snap.DataTo(dst)
}

// SaveRaw save raw byte data
func (k *Firestore) SaveRaw(key string, val []byte, store string) error {
	data := map[string]interface{}{}
	json.Unmarshal(val, data)
	if _, err := k.Save(key, store, data); err != nil {
		return err
	}
	return nil
}

// Save a key to a collection
func (k *Firestore) Save(key, store string, src interface{}) (string, error) {
	// data, err := json.Marshal(src)
	// if err != nil {
	// 	return "", err
	// }
	// storeKey := k.keyForTableId(store, key)
	col := k.fs.Collection(store)
	dc := Counter{3}
	docRef := col.Doc("DCounter")
	dc.initCounter(k.ctx, docRef)
	_, err := col.Doc(key).Set(k.ctx, src)
	if err != nil {
		return "", err
	}
	dc.incrementCounter(k.ctx, docRef)
	// k.Indexer.IndexDocument(key, IndexedData{store, src})
	return key, err
}

func (k *Firestore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	panic("not implemented")
}

func (k *Firestore) Update(key string, store string, src interface{}) error {
	updates := []firestore.Update{}
	switch up := src.(type) {
	case map[string]interface{}:
		for k, v := range up {
			updates = append(updates, firestore.Update{Path: k, Value: v})
		}
	}
	_, err := k.fs.Collection(store).Doc(key).Update(k.ctx, updates)
	return err
}

func (k *Firestore) Replace(key string, store string, src interface{}) error {
	_, err := k.fs.Collection(store).Doc(key).Set(k.ctx, src)
	return err
}

func (k *Firestore) Delete(key string, store string) error {

	col := k.fs.Collection(store)
	dc := Counter{3}
	docRef := col.Doc("DCounter")
	dc.initCounter(k.ctx, docRef)
	_, err := col.Doc(key).Delete(k.ctx)
	if err == nil {
		dc.decrementCounter(k.ctx, docRef)
	}
	return err
}

func (k *Firestore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k *Firestore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k *Firestore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k *Firestore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k *Firestore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, gostore.AggregateResult, error) {
	col := k.fs.Collection(store)

	if len(filter) > 0 {
		q := firestore.Query{}
		queries := GetQueries(filter)
		for _, v := range queries {
			q = col.Where(v.field, v.op, v.val)
		}
		col.Query = q
	}
	if opts != nil {
		if orderBy := opts.GetOrderBy(); len(orderBy) > 0 {
			col = OrderQuery(orderBy, col)
		}
	}
	iter := col.Limit(count).Offset(skip).Documents(k.ctx)
	return &TransactionRows{iter, nil}, nil, nil
}
func (k *Firestore) FilterDelete(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k *Firestore) FilterCount(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	panic("not implemented")
}

func (k *Firestore) GetByField(name string, val string, store string, dst interface{}) error {
	panic("not implemented")
}

func (k *Firestore) GetByFieldsByField(name string, val string, store string, fields []string, dst interface{}) (err error) {
	panic("not implemented")
}
func (k *Firestore) BatchInsert(data []interface{}, store string, opts gostore.ObjectStoreOptions) (keys []string, err error) {
	if len(data) > 500 {
		return nil, errors.New("batch limit exceeded")
	}
	keys = make([]string, len(data))
	batch := k.fs.Batch()
	col := k.fs.Collection(store)
	dc := Counter{3}
	docRef := col.Doc("DCounter")
	dc.initCounter(k.ctx, docRef)

	for i, src := range data {
		var key string
		if _v, ok := src.(map[string]interface{}); ok {
			if k, ok := _v["id"].(string); ok {
				key = k
			} else {
				key = gostore.NewObjectId().String()
				_v["id"] = key
			}
		} else if _v, ok := src.(common.HasID); ok {
			key = _v.GetId()
		} else {
			key = gostore.NewObjectId().String()
		}
		ref := col.Doc(key)
		batch.Set(ref, src)
		keys[i] = key
	}
	_, err = batch.Commit(k.ctx)
	if err == nil {
		for _ = range data {
			dc.incrementCounter(k.ctx, docRef)
		}
	}
	return keys, err

}
func (k *Firestore) BatchDelete(ids []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	panic("not implemented")
}

func (k *Firestore) BatchUpdate(id []interface{}, data []interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k *Firestore) BatchFilterDelete(filter []map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k *Firestore) Close() {
	k.fs.Close()
}
