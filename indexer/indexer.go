package indexer

import (
	"errors"
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/mgutz/logxi/v1"
)

var logger = log.New("gostore-contrib.indexer")

type Indexer struct {
	index bleve.Index
}

func (i *Indexer) AddDocumentMapping(name string, dm *mapping.DocumentMapping) {
	// i.index.AddDocumentMapping(name, dm)
}

func (i Indexer) IndexDocument(id string, data interface{}) error {
	if i.index == nil {
		return errors.New("No index")
	}
	logger.Debug("Indexing document", "id", id, "data", data)
	return i.index.Index(id, data)
}

func (i Indexer) UnIndexDocument(id string) error {
	if i.index == nil {
		return errors.New("No index")
	}
	logger.Debug("UnIndexing document", "id", id)
	return i.index.Delete(id)
}

func (i Indexer) QueryMap(q map[string]interface{}) (*bleve.SearchResult, error) {
	queryString := ""
	for k, v := range q {
		queryString = fmt.Sprintf("%s %s:%v", queryString, k, v)
	}
	return i.Query(queryString)
}
func (i Indexer) Query(q string) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("No index")
	}
	println(q)
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	return i.index.Search(searchRequest)
}

func (i Indexer) QueryWithOptions(q string, size, from int, explain bool, fields []string) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("No index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	return i.index.Search(searchRequest)
}

func (i Indexer) QueryWithOptionsHighlighted(q string, size, from int, explain bool, fields []string) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("No index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	searchRequest.Highlight = bleve.NewHighlightWithStyle("ansi")
	return i.index.Search(searchRequest)
}

func (i Indexer) MatchQuery(q, field string) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("No index")
	}
	query := bleve.NewMatchQuery(q)
	query.SetField(field)
	query.SetFuzziness(0)
	searchRequest := bleve.NewSearchRequest(query)
	return i.index.Search(searchRequest)
}

func (i Indexer) TermQuery(q string) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("No index")
	}
	query := bleve.NewTermQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	return i.index.Search(searchRequest)
}

func (i Indexer) MatchPhraseQuery(q string) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("No index")
	}
	query := bleve.NewMatchPhraseQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	return i.index.Search(searchRequest)
}

func (i Indexer) Close() {

	if i.index == nil {
		return
	}
	i.index.Close()
}

func GetIndex(indexPath string) (bleve.Index, bool) {
	index, err := bleve.Open(indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		logger.Debug("Index path does not exist", "path", "indexPath")
		return nil, false
	}
	return index, true
}
func NewIndexerFromIndex(index bleve.Index) *Indexer {
	return &Indexer{index}
}
func NewIndexer(indexPath string, indexMapping mapping.IndexMapping) *Indexer {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening indexpath", "path", indexPath, "verbose", string(err.Error()))
	}
	if err == bleve.ErrorIndexPathDoesNotExist {
		logger.Debug(fmt.Sprintf("Creating new index at %s ...", indexPath))
		// indexMapping.DefaultAnalyzer = "keyword"
		index, err = bleve.New(indexPath, indexMapping)
		if err != nil {
			logger.Warn("Index could not be created", "path", indexPath, "err", string(err.Error()))
			if err != bleve.ErrorIndexPathExists {
				panic(err)
			}
			return nil
		}
		return &Indexer{index}
	}
	return &Indexer{index}
}
