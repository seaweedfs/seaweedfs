package util

// DrainChannel performs a non-blocking drain of a channel after receiving
// the first message. Returns all drained messages including the first one.
// The first message is passed in explicitly (already received by the caller).
func DrainChannel[T any](ch chan T, first T) []T {
	result := []T{first}
	for {
		select {
		case v := <-ch:
			result = append(result, v)
		default:
			return result
		}
	}
}

// ReorderToFront returns a new slice with every element present in frontMap
// pulled to the front while keeping the relative order seen in inputSlice
// within each partition. Items not in frontMap keep their relative order
// behind the moved-up items. Useful for prioritizing a subset of candidates
// (e.g. local replicas) without disturbing the shuffle order of the rest.
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
