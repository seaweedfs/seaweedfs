package util

func ReorderSliceByPriority[T comparable](priorityMap map[T]bool, inputSlice []T) []T {
	var prioritized []T
	var remaining []T

	for _, item := range inputSlice {
		if priorityMap[item] {
			prioritized = append(prioritized, item)
		} else {
			remaining = append(remaining, item)
		}
	}

	return append(prioritized, remaining...)
}
