package bolt

import (
	"encoding/json"
	"github.com/osiloke/gostore"
	"sync"
)

func newBoltRows(rows [][][]byte) BoltRows {
	total := len(rows)
	closed := make(chan bool)
	retrieved := make(chan string)
	nextItem := make(chan interface{})
	ci := 0
	b := BoltRows{nextItem: nextItem, closed: closed, retrieved: retrieved}
	go func() {
		defer b.Close()
	OUTER:
		for {
			select {
			case <-closed:
				logger.Info("newBoltRows closed")
				break OUTER
				return
			case item := <-nextItem:
				// logger.Info("current index", "ci", ci, "total", total)
				if ci == total {
					b.lastError = gostore.ErrEOF
					// logger.Info("break bolt rows loop")
					break OUTER
					return
				} else {
					current := rows[ci]
					if err := json.Unmarshal(current[1], item); err != nil {
						logger.Warn(err.Error())
						b.lastError = err
						retrieved <- ""
						break OUTER
						return
					} else {
						retrieved <- string(current[0])
						ci++
					}
				}
			}
		}
	}()
	return b
}

//New Api
type BoltRows struct {
	rows      [][][]byte
	i         int
	length    int
	retrieved chan string
	closed    chan bool
	nextItem  chan interface{}
	lastError error
	isClosed  bool
	sync.RWMutex
}

func (s BoltRows) Next(dst interface{}) (bool, error) {
	if s.lastError != nil {
		return false, s.lastError
	}
	//NOTE: Consider saving id in bolt data
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

func (s BoltRows) NextRaw() ([]byte, bool) {
	return nil, false
}
func (s BoltRows) LastError() error {
	return s.lastError
}
func (s BoltRows) Close() {
	// s.rows = nil
	// s.closed <- true
	logger.Info("close bolt rows")
	close(s.closed)
	close(s.retrieved)
	close(s.nextItem)
	// s.isClosed = true
}

// SyncRows synchroniously get rows
type SyncRows struct {
	length    int
	name      string
	rows      [][][]byte
	ci        int
	lastError error
}

// Next get next item
func (s *SyncRows) Next(dst interface{}) (bool, error) {
	err := gostore.ErrEOF
	if s.ci != s.length {
		row := s.rows[s.ci]
		logger.Debug("SyncRows next row", "row", row)
		err = json.Unmarshal(row[1], dst)
		if err == nil {
			s.ci++
			return true, nil
		}
		logger.Warn("SyncRows error " + err.Error())
	}
	s.lastError = err
	return false, err
}

// NextRaw get next raw item
func (s *SyncRows) NextRaw() ([]byte, bool) {
	err := gostore.ErrEOF
	if int(s.ci) != s.length {
		row := s.rows[s.ci]
		logger.Debug("SyncRows next row", "row", row)
		s.ci++
		return row[1], true
	}
	s.lastError = err
	return nil, false
}

// LastError get last error
func (s *SyncRows) LastError() error {
	return nil
}

// Count returns count of entries
func (s *SyncRows) Count() int {
	return int(s.length)
}

// Close closes row iterator
func (s *SyncRows) Close() {
	logger.Debug("finished processing rows", "result", s.rows)
}

func newSyncRows(rows [][][]byte) *SyncRows {
	total := len(rows)
	return &SyncRows{length: total, rows: rows}
}
