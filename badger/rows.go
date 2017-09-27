package badger

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	badgerdb "github.com/dgraph-io/badger"
	"github.com/osiloke/gostore"
)

//BadgerRows handles pulling row items in a goroutine
type BadgerRows struct {
	i         int
	length    int
	retrieved chan string
	closed    chan bool
	nextItem  chan interface{}
	itr       *badgerdb.Iterator
	lastError error
	isClosed  bool
	sync.RWMutex
}

// Next get next item
func (s BadgerRows) Next(dst interface{}) (bool, error) {
	if s.lastError != nil {
		return false, s.lastError
	}
	//NOTE: Consider saving id in badger data
	var _dst map[string]interface{}
	s.nextItem <- &_dst
	key := <-s.retrieved
	if key == "" {
		return false, nil
	}
	_dst["id"] = key
	_data, _ := json.Marshal(&_dst)
	json.Unmarshal(_data, dst)
	return true, nil
}

// NextRaw get next raw item
func (s BadgerRows) NextRaw() ([]byte, bool) {
	return nil, false
}

// LastError get last error
func (s BadgerRows) LastError() error {
	return s.lastError
}

// Close closes row iterator
func (s BadgerRows) Close() {
	// s.rows = nil
	// s.closed <- true
	logger.Info("close badger rows")
	close(s.closed)
	close(s.retrieved)
	close(s.nextItem)
	// s.isClosed = true
}

func newBadgerRows(itr *badgerdb.Iterator) BadgerRows {
	closed := make(chan bool)
	retrieved := make(chan string)
	nextItem := make(chan interface{})
	ci := 0
	b := BadgerRows{
		nextItem:  nextItem,
		closed:    closed,
		itr:       itr,
		retrieved: retrieved}

	go func() {

	OUTER:
		for {
			select {
			case <-closed:
				logger.Info("newBadgerRows closed")
				break OUTER
			case item := <-nextItem:
				// logger.Info("current index", "ci", ci, "total", total)
				// if ci == total {
				// 	b.lastError = gostore.ErrEOF
				// 	// logger.Info("break badger rows loop")
				// 	break OUTER
				// 	return
				// } else {
				if itr.Valid() {
					itr.Next()
					_item := itr.Item()
					var val []byte
					err := _item.Value(func(v []byte) error {
						val = make([]byte, len(v))
						copy(val, v)
						return nil
					})
					if err != nil {
						logger.Warn(fmt.Sprintf("Error while getting value for key: %q", _item.Key()))
					}
					err = json.Unmarshal(val, item)
					if err != nil {
						logger.Warn(err.Error())
						b.lastError = err
						retrieved <- ""
						break OUTER
					} else {
						key := strings.Split(string(_item.Key()), "|")[1]
						retrieved <- key
						ci++
					}
				} else {
					b.lastError = gostore.ErrEOF
					break OUTER
				}
			}
			// }
		}
		b.Close()
	}()
	return b
}
