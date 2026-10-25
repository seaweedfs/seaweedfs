package s3api

import "testing"

func TestValidateRoleSessionName(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
		// wantCode is checked only when wantErr is true
		wantCode STSErrorCode
	}{
		{"empty rejected", "", true, STSErrMissingParameter},
		{"single char rejected (below min len 2)", "a", true, STSErrInvalidParameterValue},
		{"min length 2 accepted", "ab", false, ""},
		{"plain ascii accepted", "session-name_1", false, ""},
		{"all special chars allowed", "+=,.@-", false, ""},
		{"email-style accepted", "alice@example.com", false, ""},
		{"max length 64 accepted", string(make([]byte, 64)), true, STSErrInvalidParameterValue}, // zero bytes -> invalid charset
		{"max length 64 valid charset accepted", repeat('a', 64), false, ""},
		{"length 65 rejected", repeat('a', 65), true, STSErrInvalidParameterValue},
		{"space rejected", "alice bob", true, STSErrInvalidParameterValue},
		{"slash rejected", "alice/bob", true, STSErrInvalidParameterValue},
		{"colon rejected", "alice:bob", true, STSErrInvalidParameterValue},
		{"unicode rejected", "alicé", true, STSErrInvalidParameterValue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, err := validateRoleSessionName(tc.input)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Fatalf("err mismatch: got=%v want=%v (err=%v)", gotErr, tc.wantErr, err)
			}
			if tc.wantErr && code != tc.wantCode {
				t.Fatalf("code mismatch: got=%s want=%s", code, tc.wantCode)
			}
		})
	}
}

func repeat(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}
