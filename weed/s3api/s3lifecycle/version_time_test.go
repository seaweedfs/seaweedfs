package s3lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// CompareVersionIds is the version-id tiebreak the router falls back to
// when two versions land at the same mtime second; getting the format /
// null / mixed-format axes wrong silently inverts retention rankings,
// which would resurrect deleted versions or evict live ones. The tests
// below pin every documented branch.

const (
	// 16-char hex prefix above 0x4000000000000000 → new-format inverted
	// timestamp.
	newFormatPrefix1 = "4123456789abcdef"
	newFormatPrefix2 = "5123456789abcdef" // strictly larger than prefix1
	// 16-char hex prefix at or below the threshold → old-format raw
	// timestamp.
	oldFormatPrefix1 = "0123456789abcdef"
	oldFormatPrefix2 = "0223456789abcdef" // strictly larger than prefix1
)

func TestCompareVersionIds_EqualReturnsZero(t *testing.T) {
	assert.Equal(t, 0, CompareVersionIds("abc", "abc"))
	assert.Equal(t, 0, CompareVersionIds("null", "null"))
	assert.Equal(t, 0, CompareVersionIds("", ""))
}

func TestCompareVersionIds_NullSortsLast(t *testing.T) {
	// "null" represents the bare pre-versioning entry; the router treats
	// it as older than any real version-id so genuine versions take
	// precedence in tiebreaks.
	assert.Equal(t, 1, CompareVersionIds("null", newFormatPrefix1))
	assert.Equal(t, 1, CompareVersionIds("null", oldFormatPrefix1))
	assert.Equal(t, -1, CompareVersionIds(newFormatPrefix1, "null"))
	assert.Equal(t, -1, CompareVersionIds(oldFormatPrefix1, "null"))
}

func TestCompareVersionIds_BothNewFormatSmallerIsNewer(t *testing.T) {
	// New-format encodes the timestamp inverted, so the lexicographically
	// smaller string represents a newer wall-clock time.
	assert.Equal(t, -1, CompareVersionIds(newFormatPrefix1, newFormatPrefix2),
		"new-format: smaller lex → newer → -1")
	assert.Equal(t, 1, CompareVersionIds(newFormatPrefix2, newFormatPrefix1),
		"new-format: larger lex → older → 1")
}

func TestCompareVersionIds_BothOldFormatLargerIsNewer(t *testing.T) {
	// Old-format encodes the timestamp raw, so the lexicographically
	// larger string is the newer wall-clock time.
	assert.Equal(t, 1, CompareVersionIds(oldFormatPrefix1, oldFormatPrefix2),
		"old-format: smaller lex → older → 1")
	assert.Equal(t, -1, CompareVersionIds(oldFormatPrefix2, oldFormatPrefix1),
		"old-format: larger lex → newer → -1")
}

func TestCompareVersionIds_MixedFormatComparesByTimestamp(t *testing.T) {
	// New-format inverted timestamp = MaxInt64 - prefix; old-format raw =
	// prefix. With prefix1 = 0x4123… and old prefix1 = 0x0123… the
	// inverted new value is small but still positive, while the raw old
	// value is also small. Whichever is numerically larger wins.
	at := getVersionTimestamp(newFormatPrefix1)
	bt := getVersionTimestamp(oldFormatPrefix1)
	require := assert.New(t)
	if at == bt {
		require.Equal(0, CompareVersionIds(newFormatPrefix1, oldFormatPrefix1))
		return
	}
	if at > bt {
		require.Equal(-1, CompareVersionIds(newFormatPrefix1, oldFormatPrefix1))
		require.Equal(1, CompareVersionIds(oldFormatPrefix1, newFormatPrefix1))
	} else {
		require.Equal(1, CompareVersionIds(newFormatPrefix1, oldFormatPrefix1))
		require.Equal(-1, CompareVersionIds(oldFormatPrefix1, newFormatPrefix1))
	}
}

func TestCompareVersionIds_MixedFormatEqualTimestampReturnsZero(t *testing.T) {
	// If a new-format inverted timestamp equals an old-format raw
	// timestamp, neither is newer — should be 0. We can synthesize this:
	// pick an old prefix P, the matching new prefix is MaxInt64 - P.
	old := uint64(0x0000000000010000)
	new_ := uint64(int64(^uint64(0)>>1) - int64(old))
	oldHex := uintToHex16(old)
	newHex := uintToHex16(new_)
	// Sanity: new_ must be above the threshold, otherwise it'd be parsed
	// as old-format and the test wouldn't exercise the mixed branch.
	if !isNewFormatVersionId(newHex) {
		t.Fatalf("constructed new-format prefix %q didn't classify as new", newHex)
	}
	if isNewFormatVersionId(oldHex) {
		t.Fatalf("constructed old-format prefix %q classified as new", oldHex)
	}
	assert.Equal(t, 0, CompareVersionIds(newHex, oldHex), "mixed-format equal timestamps must be 0")
}

func TestIsNewFormatVersionId_TooShortRejected(t *testing.T) {
	// Strings shorter than 16 chars can't carry a hex timestamp prefix;
	// they must classify as old-format so getVersionTimestamp returns 0
	// rather than panic on slice bounds.
	assert.False(t, isNewFormatVersionId(""))
	assert.False(t, isNewFormatVersionId("4123"))
	assert.False(t, isNewFormatVersionId("4123456789abcde")) // 15 chars
}

func TestIsNewFormatVersionId_NullRejected(t *testing.T) {
	assert.False(t, isNewFormatVersionId("null"))
}

func TestIsNewFormatVersionId_NonHexRejected(t *testing.T) {
	// Parse failure on the 16-char prefix means the comparator falls
	// back to old-format treatment. Non-hex characters or surrounding
	// whitespace must reject without erroring out.
	assert.False(t, isNewFormatVersionId("zzzzzzzzzzzzzzzz"))
	assert.False(t, isNewFormatVersionId("0xff000000000000")) // "0x" prefix breaks ParseUint base-16
	assert.False(t, isNewFormatVersionId("ABC GHIJKLMNOPQR")) // space mid-string
}

func TestIsNewFormatVersionId_ThresholdBoundary(t *testing.T) {
	// versionIdFormatThreshold = 0x4000000000000000. The check is
	// strictly greater than, so the threshold value itself must
	// classify as old-format.
	assert.False(t, isNewFormatVersionId("4000000000000000"), "exactly at threshold is old")
	assert.True(t, isNewFormatVersionId("4000000000000001"), "one above threshold is new")
	assert.False(t, isNewFormatVersionId("3fffffffffffffff"), "one below threshold is old")
}

func TestGetVersionTimestamp_ReturnsZeroOnMalformed(t *testing.T) {
	assert.Equal(t, int64(0), getVersionTimestamp(""))
	assert.Equal(t, int64(0), getVersionTimestamp("null"))
	assert.Equal(t, int64(0), getVersionTimestamp("4123456789abcde"))      // too short
	assert.Equal(t, int64(0), getVersionTimestamp("zzzzzzzzzzzzzzzz"))     // not hex
	assert.Equal(t, int64(0), getVersionTimestamp("zzzzzzzzzzzzzzzzMORE")) // not hex prefix
}

func TestGetVersionTimestamp_OldFormatReturnsRawValue(t *testing.T) {
	// Old-format: prefix below threshold, returned raw.
	assert.Equal(t, int64(0x0123456789abcdef), getVersionTimestamp(oldFormatPrefix1))
	assert.Equal(t, int64(0x0223456789abcdef), getVersionTimestamp(oldFormatPrefix2))
	// Trailing characters past the 16-char prefix are ignored.
	assert.Equal(t, int64(0x0123456789abcdef), getVersionTimestamp(oldFormatPrefix1+"-suffix"))
}

func TestGetVersionTimestamp_NewFormatReturnsInvertedValue(t *testing.T) {
	// New-format inversion: int64 max - parsed value. So a smaller
	// stored prefix yields a larger inverted timestamp (= newer time).
	maxI64 := int64(^uint64(0) >> 1)
	assert.Equal(t, maxI64-int64(0x4123456789abcdef), getVersionTimestamp(newFormatPrefix1))
	assert.Equal(t, maxI64-int64(0x5123456789abcdef), getVersionTimestamp(newFormatPrefix2))
	// The smaller-prefix-=-newer-timestamp invariant the comparator
	// relies on:
	assert.Greater(t, getVersionTimestamp(newFormatPrefix1), getVersionTimestamp(newFormatPrefix2))
}

// uintToHex16 turns a uint64 into a 16-char lowercase hex string,
// padding with leading zeros. Used for synthesizing mixed-format
// equal-timestamp pairs in TestCompareVersionIds_MixedFormatEqualTimestampReturnsZero.
func uintToHex16(v uint64) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		out[i] = hex[v&0xf]
		v >>= 4
	}
	return string(out)
}
