package bolt

import (
	"fmt"
	"strconv"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	logger.Debug(fmt.Sprintf("%s took %d", name, elapsed))
}

func IsInt(v string) bool {
	if _, err := strconv.Atoi(v); err == nil {
		return true
	}
	return false
}
