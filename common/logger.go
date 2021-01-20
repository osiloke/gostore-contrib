package common

import (
	log "github.com/mgutz/logxi/v1"
)

var Logger = func(name string) log.Logger {
	return log.New("gostore::" + name)
}
