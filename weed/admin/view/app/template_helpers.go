package app

import (
	"fmt"
	"strconv"
)

// getStatusColor returns Bootstrap color class for status
func getStatusColor(status string) string {
	switch status {
	case "active", "healthy":
		return "success"
	case "warning":
		return "warning"
	case "critical", "unreachable":
		return "danger"
	default:
		return "secondary"
	}
}

// formatBytes converts bytes to human readable format
func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	var i int
	value := float64(bytes)

	for value >= 1024 && i < len(units)-1 {
		value /= 1024
		i++
	}

	if i == 0 {
		return fmt.Sprintf("%.0f %s", value, units[i])
	}
	return fmt.Sprintf("%.1f %s", value, units[i])
}

// formatNumber formats large numbers with commas
func formatNumber(num int64) string {
	if num == 0 {
		return "0"
	}

	str := strconv.FormatInt(num, 10)
	result := ""

	for i, char := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(char)
	}

	return result
}

// calculatePercent calculates percentage for progress bars
func calculatePercent(current, max int) int {
	if max == 0 {
		return 0
	}
	return (current * 100) / max
}
