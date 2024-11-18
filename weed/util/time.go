package util

import (
	"time"
)

func GetNextDayTsNano(curTs int64) int64 {
	curTime := time.Unix(0, curTs)
	nextDay := curTime.AddDate(0, 0, 1).Truncate(24 * time.Hour)
	nextDayNano := nextDay.UnixNano()

	return nextDayNano
}
