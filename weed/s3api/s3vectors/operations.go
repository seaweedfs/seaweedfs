package s3vectors

import "sort"

func Operations() []string {
	operations := make([]string, 0, len(handlers))
	for operation := range handlers {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	return operations
}
