package bolt

import (
	"encoding/json"
	"github.com/blevesearch/bleve"
	"github.com/osiloke/gostore"
	// "github.com/osiloke/gostore-contrib/indexer"
)

type NextItem struct {
	key    string
	target interface{}
}

//New Api
type IndexedBoltRows struct {
	lastError error
	isClosed  bool
	closed    chan bool
	retrieved chan string
	nextItem  chan interface{}
}

func (s IndexedBoltRows) Next(dst interface{}) (bool, error) {
	if s.lastError != nil {
		return false, s.lastError
	}

	s.nextItem <- dst
	key := <-s.retrieved
	if key == "" {
		return false, nil
	}
	return true, nil
}

func (s IndexedBoltRows) NextRaw() ([]byte, bool) {
	return nil, false
}
func (s IndexedBoltRows) LastError() error {
	return s.lastError
}
func (s IndexedBoltRows) Close() {
	// s.rows = nil
	if s.isClosed {
		return
	}
	s.closed <- true
	logger.Info("close bolt rows")
	s.isClosed = true
}
func NewIndexedBoltRows(name string, total uint64, result *bleve.SearchResult, bs *BoltStore) IndexedBoltRows {
	closed := make(chan bool, 1)
	nextItem := make(chan interface{})
	retrieved := make(chan string)
	ci := 0

	b := IndexedBoltRows{isClosed: false, nextItem: nextItem, closed: closed, retrieved: retrieved}
	go func() {
	OUTER:
		for {
			select {
			case <-closed:
				logger.Info("newIndexedBoltRows closed")
				close(closed)
				break OUTER

			case item := <-nextItem:
				logger.Info("current index", "ci", ci, "total", total)
				if uint64(ci) == total {
					b.lastError = gostore.ErrEOF
					logger.Info("break bolt rows loop")
					retrieved <- ""
					break OUTER

				} else {
					h := result.Hits[ci]
					logger.Info("retrieving row", "row", h)
					row, err := bs._Get(h.ID, name)
					if err != nil {
						if err == gostore.ErrNotFound {
							//not found so remove from indexer
							bs.Indexer.UnIndexDocument(h.ID)
							retrieved <- ""
							continue
						} else {
							logger.Warn(err.Error())
							b.lastError = err
							retrieved <- ""
							break OUTER
						}

					}
					if err := json.Unmarshal(row[1], item); err != nil {
						logger.Warn(err.Error())
						b.lastError = err
						retrieved <- ""
						break OUTER

					}
					retrieved <- string(row[0])
					ci++
				}
			}
		}
		close(retrieved)
		close(nextItem)
		// close(closed)
	}()
	return b
}
