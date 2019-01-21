package util

func EmptyThen(s1, s2 string) string {
	if s1 == "" {
		return s2
	}

	return s1
}
