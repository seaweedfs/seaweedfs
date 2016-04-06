package util

import (
	"fmt"
	"time"
)

func FormatDuration(d time.Duration) string {
	if days := d / (time.Hour * 24); days > 0 {
		hours := d % (time.Hour * 24) / time.Hour
		return fmt.Sprintf("%d days %d hours", int(days), int(hours))
	} else if hours := d / time.Hour; hours > 0 {
		minutes := d % time.Hour / time.Minute
		return fmt.Sprintf("%d hours %d minutes", int(hours), int(minutes))
	} else {
		minutes := d / time.Minute
		seconds := d % time.Minute / time.Second
		return fmt.Sprintf("%d minutes %d seconds", int(minutes), int(seconds))
	}
	return d.String()
}
