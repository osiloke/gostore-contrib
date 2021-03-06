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

func Test_addRangeFacets(t *testing.T) {
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
			"OneRangeFacet",
			args{
				bleve.NewSearchRequest(q),
				&Facets{
					Range: map[string]RangeFacet{
						"waterRating": RangeFacet{
							Field: "ratings.water",
							Ranges: []interface{}{
								map[string]interface{}{"name": "1", "min": 1, "max": 1},
								map[string]interface{}{"name": "2", "min": 2, "max": 2},
								map[string]interface{}{"name": "3", "min": 3, "max": 3},
							},
						},
					},
				},
			},
			false,
			bleve.FacetsRequest{"topActiveCars": &bleve.FacetRequest{
				Field: "ratings.water",
				Size:  3,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := addRangeFacets(tt.args.searchRequest, tt.args.facets); (err != nil) != tt.wantErr {
				t.Errorf("addRangeFacets() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				assert.Equal(t, tt.expected, tt.args.searchRequest.Facets, "does not contain added facet")
			}
		})
	}
}

func TestGetQueryString(t *testing.T) {
	type args struct {
		store  string
		filter map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"day": "<:d2019-06-11T12:13:43.523888755Z"},
			},
			`+bucket:store +data.day:<"2019-06-11T12:13:43.523888755Z"`,
		},
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"day": ">:d2019-06-11T12:13:43.523888755Z"},
			},
			`+bucket:store +data.day:>"2019-06-11T12:13:43.523888755Z"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetQueryString(tt.args.store, tt.args.filter); got != tt.want {
				t.Errorf("GetQueryString() = %v, want %v", got, tt.want)
			}
		})
	}
}
