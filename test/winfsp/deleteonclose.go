package winfsp

import "errors"

// errWindowsOnly marks the platforms with no way to ask for delete-on-close,
// so the test can tell that apart from the flag being refused.
var errWindowsOnly = errors.New("delete-on-close is a windows flag")
