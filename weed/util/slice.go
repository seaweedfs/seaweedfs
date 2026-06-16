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
