package app

import (
	"fmt"
	"math"
)

// formatNumber formats a number with thousand separators
func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	length := len(str)
	if length <= 3 {
		return str
	}

	var result string
	for i, ch := range str {
		if i > 0 && (length-i)%3 == 0 {
			result += ","
		}
		result += string(ch)
	}
	return result
}

// formatBytes formats bytes in human-readable format
func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	units := []string{"B", "KB", "MB", "GB", "TB"}
	divisor := float64(1024)
	index := 0

	size := float64(bytes)
	for size >= divisor && index < len(units)-1 {
		size /= divisor
		index++
	}

	if index == 0 {
		return fmt.Sprintf("%d %s", int64(size), units[index])
	}
	return fmt.Sprintf("%.2f %s", size, units[index])
}

// calculatePercent calculates percentage
func calculatePercent(current, total int64) float64 {
	if total == 0 {
		return 0
	}
	return math.Round((float64(current)/float64(total))*100*100) / 100
}
