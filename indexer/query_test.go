package indexer

import (
	"testing"

	"github.com/blevesearch/bleve"
	"github.com/stretchr/testify/assert"
)

func TestAddFacets(t *testing.T) {
	type args struct {
		searchRequest *bleve.SearchRequest
		facets        *Facets
	}
	q := bleve.NewQueryStringQuery("")
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		expected bleve.FacetsRequest
	}{
		{
			"TopFacet",
			args{
				bleve.NewSearchRequest(q),
				&Facets{
					Top: map[string]TopFacet{
						"topActiveCars": TopFacet{Name: "topActiveCars", Field: "car", Count: 5},
					},
				},
			},
			false,
			bleve.FacetsRequest{"topActiveCars": &bleve.FacetRequest{Field: "car", Size: 5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AddFacets(tt.args.searchRequest, tt.args.facets); (err != nil) != tt.wantErr {
				t.Errorf("AddFacets() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				assert.Equal(t, tt.expected, tt.args.searchRequest.Facets, "does not contain added facet")
			}
		})
	}
}
