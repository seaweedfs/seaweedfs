//go:debug fips140=only

package main

import (
	"crypto/fips140"
	"testing"
)

// TestInitUnderFIPS140Only runs with GODEBUG=fips140=only and links every
// package the weed binary imports, so package initialization that uses an
// algorithm FIPS 140-3 does not approve (such as SHA-1) panics before this
// test runs and fails the package.
func TestInitUnderFIPS140Only(t *testing.T) {
	if !fips140.Enforced() {
		t.Fatal("fips140=only is not in effect; the //go:debug directive above was not applied")
	}
}
