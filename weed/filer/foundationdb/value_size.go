package foundationdb

import "fmt"

// FDB_VALUE_SIZE_LIMIT is Apple's documented maximum value size: 100,000
// bytes, not 100 KiB. A value over it is rejected by FoundationDB itself with
// error 2103, so the store checks it and names it.
const FDB_VALUE_SIZE_LIMIT = 100 * 1000

func errIfValueTooLarge(name string, value []byte) error {
	if len(value) > FDB_VALUE_SIZE_LIMIT {
		return fmt.Errorf("entry %s exceeds FoundationDB value size limit (%d > %d bytes)",
			name, len(value), FDB_VALUE_SIZE_LIMIT)
	}
	return nil
}
