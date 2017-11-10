package indexer

type TopFacet struct {
	Name  string `json:"name"`
	Field string `json:"field"`
	Count int    `json:"count"`
}
type RangeFacet struct {
	Name  string `json:"name"`
	Field string `json:"field"`
	Count int    `json:"count"`
}

type Facets struct {
	Top   map[string]TopFacet   `json:"top"`
	Range map[string]RangeFacet `json:"range"`
}
