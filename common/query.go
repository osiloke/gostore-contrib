package common

import (
	"fmt"
	"github.com/mgutz/logxi/v1"
	"strings"
)

var logger = log.New("gostore-contrib.common")

func GetQueryString(store string, filter map[string]interface{}) string {
	queryString := "+bucket:" + store
	for k, v := range filter {
		if _v, ok := v.(int); ok {
			queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, _v)
			queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, _v)
		} else if vv, ok := v.(string); ok {
			valRune := []rune(vv)
			first := string(valRune[0])
			if first == "<" {
				v = string(valRune[1:])
				queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, v)
			} else if first == ">" {
				v = string(valRune[1:])
				queryString = fmt.Sprintf("%s +data.%s:>=%v", queryString, k, v)

			} else {
				prefix := "+"
				if first == "!" {
					prefix = "-"
					v = string(valRune[1:])
				}
				queryString = fmt.Sprintf(`%s %sdata.%s:"%v"`, queryString, prefix, k, v)
			}
		} else {
			logger.Warn(store+" QueryString ["+k+"] was not parsed", "filter", filter, "value", v)
		}
	}
	return strings.Replace(queryString, "\"", "", -1)
}
