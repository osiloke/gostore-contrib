package bolt

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

// WriteToHTTP writes store to http writer
func (s *BoltStore) WriteToHTTP(w http.ResponseWriter) error {
	date := time.Now().Format("2006_01_02_15-04-05")
	err := s.Db.View(func(tx *bolt.Tx) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="bolt_%s.db"`, date))
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		// http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	return nil
}
