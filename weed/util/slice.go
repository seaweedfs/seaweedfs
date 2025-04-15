package util

func ReorderToFront[T comparable](frontMap map[T]bool, inputSlice []T) []T {
	var prioritized []T
	var remaining []T

	for _, item := range inputSlice {
		if frontMap[item] {
			prioritized = append(prioritized, item)
		} else {
			remaining = append(remaining, item)
		}
	}

	return append(prioritized, remaining...)
}
