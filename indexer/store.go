package indexer

import (
	"github.com/osiloke/gostore-contrib/common"
)

// ProviderStore a store which provides data to an index store
type ProviderStore interface {
	Cursor() (common.Iterator, error)
}
