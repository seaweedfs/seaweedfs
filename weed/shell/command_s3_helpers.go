package shell

// joinMax joins up to max strings with ", " and appends "..." if truncated.
func joinMax(items []string, max int) string {
	if len(items) <= max {
		result := ""
		for i, s := range items {
			if i > 0 {
				result += ", "
			}
			result += s
		}
		return result
	}
	result := ""
	for i := 0; i < max; i++ {
		if i > 0 {
			result += ", "
		}
		result += items[i]
	}
	return result + "..."
}
