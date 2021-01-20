package firestoredb

import (
	"encoding/json"

	"cloud.google.com/go/firestore"
)

// TransactionRows synchroniously get rows
type TransactionRows struct {
	iter      *firestore.DocumentIterator
	lastError error
}

// Next get next item
func (s *TransactionRows) Next(dst interface{}) (bool, error) {
	snap, err := s.iter.Next()
	if err != nil {
		s.lastError = err
		return false, err
	}
	err = snap.DataTo(&dst)
	return true, err
}

// NextRaw get next raw item
func (s *TransactionRows) NextRaw() ([]byte, bool) {
	snap, err := s.iter.Next()
	if err != nil {
		s.lastError = err
		return nil, false
	}
	data := snap.Data()
	b, err := json.Marshal(data)
	if err == nil {
		return b, true
	}
	return nil, false
}

// LastError get last error
func (s *TransactionRows) LastError() error {
	return s.lastError
}

// Count returns count of entries
func (s *TransactionRows) Count() int {
	// return s.iter.
	return 0
}

// Close closes row iterator
func (s *TransactionRows) Close() {
	s.iter.Stop()
}
