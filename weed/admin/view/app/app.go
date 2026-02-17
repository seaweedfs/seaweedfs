package app

import (
	"fmt"
)

// formatNumber formats a number with commas
func formatNumber(n interface{}) string {
	switch v := n.(type) {
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.2f", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatBytes formats bytes as human-readable size
func formatBytes(bytes interface{}) string {
	switch v := bytes.(type) {
	case int:
		return formatBytesValue(int64(v))
	case int64:
		return formatBytesValue(v)
	case float64:
		return formatBytesValue(int64(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatBytesValue(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// calculatePercent calculates percentage
func calculatePercent(current, total interface{}) float64 {
	var c, t float64

	switch v := current.(type) {
	case int:
		c = float64(v)
	case int64:
		c = float64(v)
	case float64:
		c = v
	}

	switch v := total.(type) {
	case int:
		t = float64(v)
	case int64:
		t = float64(v)
	case float64:
		t = v
	}

	if t == 0 {
		return 0
	}
	return (c / t) * 100
}
