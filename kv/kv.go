package kv

import (
	"errors"
	"github.com/cznic/kv"
	"github.com/osiloke/gostore"
	"os"
)

type Kv struct {
	path  string
	store *kv.DB
}

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}
func NewKvStore(path string) Kv {
	k := Kv{path: path}
	var db *kv.DB
	var err error
	if _, err = os.Stat(k.path); os.IsNotExist(err) {
		db, _ = kv.Create(k.path, opts())
	} else {
		db, _ = kv.Open(k.path, opts())

	}
	k.store = db
	return k
}
func (k Kv) CreateDatabase() (err error) {

	return nil
}

func (k Kv) CreateTable(table string, sample interface{}) error {
	panic("not implemented")
}

func (k Kv) GetStore() interface{} {
	return k.store
}

func (k Kv) Stats(store string) (map[string]interface{}, error) {
	panic("not implemented")
}

func (k Kv) All(count int, skip int, store string) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) AllCursor(store string) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) Since(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) Before(id string, count int, skip int, store string) (gostore.ObjectRows, error) {
	// rows, err := SeekBefore(k.store, id, count, skip, store)
	return nil, errors.New("not implemented")
}

func (k Kv) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	panic("not implemented")
}

func (k Kv) Get(key string, store string, dst interface{}) error {
	panic("not implemented")
}

func (k Kv) Save(store string, src interface{}) (string, error) {
	panic("not implemented")
}

func (k Kv) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	panic("not implemented")
}

func (k Kv) Update(key string, store string, src interface{}) error {
	panic("not implemented")
}

func (k Kv) Replace(key string, store string, src interface{}) error {
	panic("not implemented")
}

func (k Kv) Delete(key string, store string) error {
	panic("not implemented")
}

func (k Kv) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k Kv) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k Kv) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k Kv) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts gostore.ObjectStoreOptions) (gostore.ObjectRows, error) {
	panic("not implemented")
}

func (k Kv) FilterDelete(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k Kv) FilterCount(filter map[string]interface{}, store string, opts gostore.ObjectStoreOptions) (int64, error) {
	panic("not implemented")
}

func (k Kv) GetByField(name string, val string, store string, dst interface{}) error {
	panic("not implemented")
}

func (k Kv) GetByFieldsByField(name string, val string, store string, fields []string, dst interface{}) (err error) {
	panic("not implemented")
}

func (k Kv) BatchDelete(ids []interface{}, store string, opts gostore.ObjectStoreOptions) (err error) {
	panic("not implemented")
}

func (k Kv) BatchUpdate(id []interface{}, data []interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k Kv) BatchFilterDelete(filter []map[string]interface{}, store string, opts gostore.ObjectStoreOptions) error {
	panic("not implemented")
}

func (k Kv) Close() {
	panic("not implemented")
}
