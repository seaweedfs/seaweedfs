package needle

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestParseKeyHash(t *testing.T) {
	testcases := []struct {
		KeyHash string
		ID      types.NeedleId
		Cookie  types.Cookie
		Err     bool
	}{
		// normal
		{"4ed4c8116e41", 0x4ed4, 0xc8116e41, false},
		// cookie with leading zeros
		{"4ed401116e41", 0x4ed4, 0x01116e41, false},
		// odd length
		{"ed400116e41", 0xed4, 0x00116e41, false},
		// uint
		{"fed4c8114ed4c811f0116e41", 0xfed4c8114ed4c811, 0xf0116e41, false},
		// err: too short
		{"4ed4c811", 0, 0, true},
		// err: too long
		{"4ed4c8114ed4c8114ed4c8111", 0, 0, true},
		// err: invalid character
		{"helloworld", 0, 0, true},
	}

	for _, tc := range testcases {
		if id, cookie, err := ParseNeedleIdCookie(tc.KeyHash); err != nil && !tc.Err {
			t.Fatalf("Parse %s error: %v", tc.KeyHash, err)
		} else if err == nil && tc.Err {
			t.Fatalf("Parse %s expected error got nil", tc.KeyHash)
		} else if id != tc.ID || cookie != tc.Cookie {
			t.Fatalf("Parse %s wrong result. Expected: (%d, %d) got: (%d, %d)", tc.KeyHash, tc.ID, tc.Cookie, id, cookie)
		}
	}
}

func BenchmarkParseKeyHash(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ParseNeedleIdCookie("4ed44ed44ed44ed4c8116e41")
	}
}
