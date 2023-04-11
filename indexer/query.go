package indexer

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve"
)

func reduceValueLenght(v string) string {
	if len(v) > 100 {
		return v[0:100]
	}
	return v
}

func formatted(prefix, field string, valRune []rune) (queryString string) {
	v := strings.TrimSpace(string(valRune[1:]))
	v = strings.Replace(v, "\"", "", -1)
	switch strings.ToLower(string(valRune[0])) {
	case "d":
		logger.Debug("date format")
		queryString = fmt.Sprintf(`+data.%s:%s"%v"`, field, prefix, v)
	case "n":
		queryString = fmt.Sprintf("+data.%s:%s=%v", field, prefix, v)
	default:
		queryString = fmt.Sprintf("+data.%s:%s%v", field, prefix, v) //this should not be supported
	}
	return strings.TrimSpace(queryString)
}

func getQueryValue(store, k string, v interface{}) string {
	queryString := ""
	if _v, ok := v.(int); ok {
		stringValue := strconv.Itoa(_v)
		queryString = fmt.Sprintf("+data.%s:>=%s", k, stringValue)
		queryString = fmt.Sprintf("%s +data.%s:<=%s", queryString, k, stringValue)
	} else if _v, ok := v.(int64); ok {
		stringValue := strconv.FormatInt(_v, 10)
		queryString = fmt.Sprintf("+data.%s:>=%s", k, stringValue)
		queryString = fmt.Sprintf("%s +data.%s:<=%s", queryString, k, stringValue)
	} else if _v, ok := v.(float64); ok {
		stringValue := strconv.Itoa(int(_v))
		queryString = fmt.Sprintf("+data.%s:>=%v", k, stringValue)
		queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, stringValue)
	} else if vv, ok := v.(string); ok {
		valRune := []rune(vv)
		var first rune
		if len(valRune) > 0 {
			first = valRune[0]
		} else {
			first = 0
		}
		if string(first) == "^" { //match ^ regex
			prefix := "+"
			queryString = fmt.Sprintf(`%sdata.%s:/%v/`, prefix, k, reduceValueLenght(string(valRune[1:])))
		} else if first == '\x3C' {
			if valRune[1] == '\x3A' {
				// something like <:d2016-12-12
				queryString = formatted("<", k, valRune[2:])
			} else {
				// something like <1
				v = string(valRune[1:])
				queryString = fmt.Sprintf("+data.%s:<=%v", k, v)
			}
		} else if first == '\x3E' {
			if valRune[1] == '\x3A' {
				// something like >:d2016-12-12
				queryString = formatted(">", k, valRune[2:])
			} else {
				// something like >20
				v = string(valRune[1:])
				queryString = fmt.Sprintf("+data.%s:>=%v", k, v)
			}
		} else {
			prefix := "+"
			if first == '\x21' {
				prefix = "-"
				v = string(valRune[1:])
			}

			queryString = fmt.Sprintf(`%sdata.%s:"%v"`, prefix, k, reduceValueLenght(string(fmt.Sprintf("%v", v))))
		}
	} else if _v, ok := v.(bool); ok {
		queryString = fmt.Sprintf(`+data.%s:"%v"`, k, _v)
	} else {
		logger.Warn(store+" QueryString ["+k+"] was not parsed - defaulting to raw text", "value", v, "type", reflect.TypeOf(v))
		queryString = fmt.Sprintf(`+data.%s:"%v"`, k, v)
	}
	return queryString
}

func GetQueryString(store string, filter map[string]interface{}) string {
	queryString := "+bucket:" + store
	for k, v := range filter {
		if _v, ok := v.(int); ok {
			stringValue := strconv.Itoa(_v)
			queryString = fmt.Sprintf("%s +data.%s:>=%s", queryString, k, stringValue)
			queryString = fmt.Sprintf("%s +data.%s:<=%s", queryString, k, stringValue)
		} else if _v, ok := v.(int64); ok {
			stringValue := strconv.FormatInt(_v, 10)
			queryString = fmt.Sprintf("%s +data.%s:>=%s", queryString, k, stringValue)
			queryString = fmt.Sprintf("%s +data.%s:<=%s", queryString, k, stringValue)
		} else if _v, ok := v.(float64); ok {
			stringValue := strconv.Itoa(int(_v))
			queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, stringValue)
			queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, stringValue)
		} else if vv, ok := v.(string); ok {
			valRune := []rune(vv)
			var first rune
			if len(valRune) > 0 {
				first = valRune[0]
			} else {
				first = 0
			}
			if string(first) == "^" { //match ^ regex
				prefix := "+"
				queryString = fmt.Sprintf(`%s %sdata.%s:/%v/`, queryString, prefix, k, reduceValueLenght(string(valRune[1:])))
			} else if first == '\x3C' {
				if valRune[1] == '\x3A' {
					// something like <:d2016-12-12
					queryString = fmt.Sprintf("%s %s", queryString, formatted("<", k, valRune[2:]))
				} else {
					// something like <1
					v = string(valRune[1:])
					queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, v)
				}
			} else if first == '\x3E' {
				if valRune[1] == '\x3A' {
					// something like >:d2016-12-12
					queryString = fmt.Sprintf("%s %s", queryString, formatted(">", k, valRune[2:]))
				} else {
					// something like >20
					v = string(valRune[1:])
					queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, v)
				}
			} else {
				prefix := "+"
				if first == '\x21' {
					prefix = "-"
					v = string(valRune[1:])
				}

				queryString = fmt.Sprintf(`%s %sdata.%s:"%v"`, queryString, prefix, k, reduceValueLenght(string(fmt.Sprintf("%v", v))))
			}
		} else if _v, ok := v.(bool); ok {
			queryString = fmt.Sprintf(`%s +data.%s:"%v"`, queryString, k, _v)
		} else if _v, ok := v.([]string); ok {
			for _, v := range _v {
				queryString = fmt.Sprintf("%s %s", queryString, getQueryValue(store, k, v))
			}
		} else {
			logger.Warn(store+" QueryString ["+k+"] was not parsed - defaulting to raw text", "filter", filter, "value", v, "type", reflect.TypeOf(v))
			queryString = fmt.Sprintf(`%s +data.%s:"%v"`, queryString, k, v)
		}
	}
	return queryString //strings.Replace(queryString, "\"", "", -1)
}

func floatVal(v interface{}) float64 {
	if vv, ok := v.(float64); ok {
		return vv
	}
	return float64(v.(int))
}

// AddRangeFacets add range dacets to request
func addRangeFacets(searchRequest *bleve.SearchRequest, facets *Facets) error {
	for k, facet := range facets.Range {
		fieldFacet := bleve.NewFacetRequest(facet.Field, len(facet.Ranges))
		for _, v := range facet.Ranges {
			numericRange := v.(map[string]interface{})
			name := numericRange["name"].(string)
			min := floatVal(numericRange["min"])
			max := floatVal(numericRange["max"])
			fieldFacet.AddNumericRange(name, &min, &max)
		}
		searchRequest.AddFacet(k, fieldFacet)
	}
	return nil
}

// AddFacets facets to a request
func AddFacets(searchRequest *bleve.SearchRequest, facets *Facets) error {
	for _, facet := range facets.Top {
		fieldFacet := bleve.NewFacetRequest(facet.Field, facet.Count)
		searchRequest.AddFacet(facet.Name, fieldFacet)
	}
	addRangeFacets(searchRequest, facets)
	return nil
}
