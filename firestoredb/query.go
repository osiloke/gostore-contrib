package firestoredb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func reduceValueLenght(v string) string {
	if len(v) > 100 {
		return v[0:100]
	}
	return v
}

func formatted(prefix, field string, valRune []rune) query {
	var q query
	v := strings.TrimSpace(string(valRune[1:]))
	v = strings.Replace(v, "\"", "", -1)
	switch strings.ToLower(string(valRune[0])) {
	case "d":
		// logger.Debug("date format", "prefix", prefix, "field", field)
		// queryString = fmt.Sprintf(`+data.%s:%s"%v"`, field, prefix, v)
		q = query{field, prefix, v}
	case "n":
		// queryString = fmt.Sprintf("+data.%s:%s=%v", field, prefix, v)
		vv, _ := strconv.Atoi(v)
		q = query{field, prefix + "=", vv}
	default:
		// queryString = fmt.Sprintf("+data.%s:%s%v", field, prefix, v) //this should not be supported
		q = query{field, prefix, v}
	}
	return q
}

type query struct {
	field string
	op    string
	val   interface{}
}

func GetQueries(filter map[string]interface{}) []query {
	queries := []query{}
	for k, v := range filter {
		if _v, ok := v.(int); ok {
			// queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, _v)
			queries = append(queries, query{k, ">=", _v})
			// queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, _v)
			queries = append(queries, query{k, "<=", _v})
		} else if _v, ok := v.(int64); ok {
			// queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, _v)
			queries = append(queries, query{k, ">=", _v})
			// queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, _v)
			queries = append(queries, query{k, "<=", _v})
		} else if _v, ok := v.(float64); ok {
			// queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, _v)
			queries = append(queries, query{k, ">=", _v})
			// queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, _v)
			queries = append(queries, query{k, "<=", _v})
		} else if vv, ok := v.(string); ok {
			valRune := []rune(vv)
			var first rune
			if len(valRune) > 0 {
				first = valRune[0]
			} else {
				first = 0
			}
			if string(first) == "^" { //match ^ regex
				prefix := "=="
				// queryString = fmt.Sprintf(`%s %sdata.%s:/%v/`, queryString, prefix, k, reduceValueLenght(string(valRune[1:])))
				queries = append(queries, query{k, prefix, reduceValueLenght(string(valRune[1:]))})
			} else if first == '\x3C' {
				if valRune[1] == '\x3A' {
					// something like <:d2016-12-12
					queries = append(queries, formatted("<", k, valRune[2:]))
				} else {
					// something like <1
					v = string(valRune[1:])
					// queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, v)
					queries = append(queries, query{k, "<=", v})
				}
			} else if first == '\x3E' {
				if valRune[1] == '\x3A' {
					// something like >:d2016-12-12
					// queryString = fmt.Sprintf("%s %s", queryString, formatted(">", k, valRune[2:]))
					queries = append(queries, formatted(">", k, valRune[2:]))
				} else {
					// something like >20
					v = string(valRune[1:])
					// queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, v)
					queries = append(queries, query{k, ">=", v})
				}
			} else {
				prefix := "=="
				if first == '\x21' {
					prefix = "not-in"
					v = string(valRune[1:])
					v = []interface{}{reduceValueLenght(string(fmt.Sprintf("%v", v)))}
				} else {
					v = reduceValueLenght(string(fmt.Sprintf("%v", v)))
				}
				// queryString = fmt.Sprintf(`%s %sdata.%s:"%v"`, queryString, prefix, k, reduceValueLenght(string(fmt.Sprintf("%v", v))))
				queries = append(queries, query{k, prefix, v})
			}
		} else {
			logger.Warn("QueryString ["+k+"] was not parsed", "filter", filter, "value", v, "type", reflect.TypeOf(v))
		}
	}
	return queries //strings.Replace(queryString, "\"", "", -1)
}

func floatVal(v interface{}) float64 {
	if vv, ok := v.(float64); ok {
		return vv
	}
	return float64(v.(int))
}
