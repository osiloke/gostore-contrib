package common

import (
	"fmt"
	"github.com/mgutz/logxi/v1"
	"strings"
)

var logger = log.New("gostore-contrib.common")

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
func GetQueryString(store string, filter map[string]interface{}) string {
	queryString := "+bucket:" + store
	for k, v := range filter {
		if _v, ok := v.(int); ok {
			queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, _v)
			queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, _v)
		} else if vv, ok := v.(string); ok {
			valRune := []rune(vv)
			first := valRune[0]
			if first == '\x3C' {
				if valRune[1] == '\x25' {
					// something like <%d2016-12-12
					queryString = fmt.Sprintf("%s %s", queryString, formatted("<", k, valRune[2:]))
				} else {
					// something like <1
					v = string(valRune[1:])
					queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, v)
				}
			} else if first == '\x3E' {
				if valRune[1] == '\x25' {
					// something like >%d2016-12-12
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
				queryString = fmt.Sprintf(`%s %sdata.%s:"%v"`, queryString, prefix, k, v)
			}
		} else {
			logger.Warn(store+" QueryString ["+k+"] was not parsed", "filter", filter, "value", v)
		}
	}
	return queryString //strings.Replace(queryString, "\"", "", -1)
}
