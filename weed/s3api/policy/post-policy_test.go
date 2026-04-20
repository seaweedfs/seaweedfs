package policy

/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	iso8601DateFormat = "20060102T150405Z"
	iso8601TimeFormat = "2006-01-02T15:04:05.000Z" // Reply date format with nanosecond precision.
)

// if object matches reserved string, no need to encode them
var reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// EncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func EncodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedPathname
}

// buildParsedPolicy is a small test helper that assembles a JSON policy
// document with the supplied condition snippets and parses it into a
// PostPolicyForm. Using ParsePostPolicyForm avoids having to construct the
// anonymous struct in PostPolicyForm.Conditions.Policies directly.
func buildParsedPolicy(t *testing.T, conditions string) PostPolicyForm {
	t.Helper()
	expiration := time.Now().UTC().Add(24 * time.Hour).Format(iso8601TimeFormat)
	raw := fmt.Sprintf(`{"expiration":"%s","conditions":[%s]}`, expiration, conditions)
	ppf, err := ParsePostPolicyForm(raw)
	if err != nil {
		t.Fatalf("ParsePostPolicyForm failed: %v\npolicy: %s", err, raw)
	}
	return ppf
}

// TestCheckPostPolicy_RejectsUnknownConditionKey verifies that a policy
// containing a condition key that is neither in startsWithConds nor prefixed
// with $x-amz- is rejected instead of being silently accepted.
func TestCheckPostPolicy_RejectsUnknownConditionKey(t *testing.T) {
	ppf := buildParsedPolicy(t, `["eq","$foo","bar"]`)

	form := http.Header{}
	form.Set("Foo", "bar")

	err := CheckPostPolicy(form, ppf)
	if err == nil {
		t.Fatalf("expected error for unknown condition key, got nil")
	}
	if !strings.Contains(err.Error(), "unknown condition key") {
		t.Fatalf("expected 'unknown condition key' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "$foo") {
		t.Fatalf("expected error to name the offending key, got: %v", err)
	}
}

// TestCheckPostPolicy_RejectsExtraXAmzFormField verifies that stray X-Amz-*
// form fields (beyond the reserved auth/signing ones) are rejected when no
// matching policy condition is declared.
func TestCheckPostPolicy_RejectsExtraXAmzFormField(t *testing.T) {
	ppf := buildParsedPolicy(t, `["eq","$bucket","mybucket"]`)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Storage-Class", "STANDARD")

	err := CheckPostPolicy(form, ppf)
	if err == nil {
		t.Fatalf("expected error for extra X-Amz-Storage-Class field, got nil")
	}
	if !strings.Contains(err.Error(), "Extra input fields") {
		t.Fatalf("expected 'Extra input fields' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "X-Amz-Storage-Class") {
		t.Fatalf("expected error to name the offending field, got: %v", err)
	}
}

// TestCheckPostPolicy_AllowsXAmzAuthFields verifies that the reserved
// auth/signing X-Amz-* headers are accepted even when no policy condition
// mentions them, because clients must always send these.
func TestCheckPostPolicy_AllowsXAmzAuthFields(t *testing.T) {
	ppf := buildParsedPolicy(t, `["eq","$bucket","mybucket"]`)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Signature", "deadbeef")
	form.Set("X-Amz-Credential", "AKIA/20260417/us-east-1/s3/aws4_request")
	form.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	form.Set("X-Amz-Date", "20260417T000000Z")
	form.Set("X-Amz-Security-Token", "session-token")

	if err := CheckPostPolicy(form, ppf); err != nil {
		t.Fatalf("expected no error with only reserved auth fields, got: %v", err)
	}
}

// TestCheckPostPolicy_AllowsMatchingXAmzField verifies that an X-Amz-* form
// field is accepted when there is a matching equality policy condition.
func TestCheckPostPolicy_AllowsMatchingXAmzField(t *testing.T) {
	ppf := buildParsedPolicy(t, `["eq","$bucket","mybucket"],["eq","$x-amz-storage-class","STANDARD"]`)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Storage-Class", "STANDARD")

	if err := CheckPostPolicy(form, ppf); err != nil {
		t.Fatalf("expected no error for matching X-Amz-Storage-Class, got: %v", err)
	}
}

// TestCheckPostPolicy_ExistingXAmzMetaCheckStillWorks is a regression test
// for the pre-existing behavior: a stray X-Amz-Meta-* form field without a
// matching condition is still rejected.
func TestCheckPostPolicy_ExistingXAmzMetaCheckStillWorks(t *testing.T) {
	ppf := buildParsedPolicy(t, `["eq","$bucket","mybucket"]`)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Meta-Foo", "bar")

	err := CheckPostPolicy(form, ppf)
	if err == nil {
		t.Fatalf("expected error for extra X-Amz-Meta-Foo field, got nil")
	}
	if !strings.Contains(err.Error(), "Extra input fields") {
		t.Fatalf("expected 'Extra input fields' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "X-Amz-Meta-Foo") {
		t.Fatalf("expected error to name the offending field, got: %v", err)
	}
}

// TestCheckPostPolicy_AllowsStartsWithPrefixStem covers the AWS convention
// where ["starts-with","$x-amz-meta-",""] permits any X-Amz-Meta-* form
// field. Without prefix-stem handling, such fields would be wrongly
// rejected as "Extra input fields".
func TestCheckPostPolicy_AllowsStartsWithPrefixStem(t *testing.T) {
	ppf := buildParsedPolicy(t,
		`["eq","$bucket","mybucket"],["starts-with","$x-amz-meta-",""]`,
	)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Meta-Foo", "bar")
	form.Set("X-Amz-Meta-Another", "baz")

	if err := CheckPostPolicy(form, ppf); err != nil {
		t.Fatalf("expected no error for prefix-matched meta fields, got: %v", err)
	}
}

// TestCheckPostPolicy_PrefixStemDoesNotCoverOtherPrefixes ensures the
// prefix allowance is scoped: a starts-with stem for x-amz-meta- must not
// whitelist unrelated x-amz-* fields like x-amz-storage-class.
func TestCheckPostPolicy_PrefixStemDoesNotCoverOtherPrefixes(t *testing.T) {
	ppf := buildParsedPolicy(t,
		`["eq","$bucket","mybucket"],["starts-with","$x-amz-meta-",""]`,
	)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Storage-Class", "STANDARD")

	err := CheckPostPolicy(form, ppf)
	if err == nil {
		t.Fatalf("expected error for X-Amz-Storage-Class not covered by meta prefix, got nil")
	}
	if !strings.Contains(err.Error(), "X-Amz-Storage-Class") {
		t.Fatalf("expected error to name X-Amz-Storage-Class, got: %v", err)
	}
}

// TestCheckPostPolicy_PrefixStemEnforcesValuePrefix covers a starts-with
// prefix-stem policy with a non-empty required value prefix: matching
// fields must have values that satisfy the value prefix.
func TestCheckPostPolicy_PrefixStemEnforcesValuePrefix(t *testing.T) {
	ppf := buildParsedPolicy(t,
		`["eq","$bucket","mybucket"],["starts-with","$x-amz-meta-","pfx-"]`,
	)

	// Value satisfies the required prefix: accepted.
	okForm := http.Header{}
	okForm.Set("Bucket", "mybucket")
	okForm.Set("X-Amz-Meta-Foo", "pfx-bar")
	if err := CheckPostPolicy(okForm, ppf); err != nil {
		t.Fatalf("expected no error when meta value matches required prefix, got: %v", err)
	}

	// Value does not satisfy the required prefix: rejected as policy failure.
	badForm := http.Header{}
	badForm.Set("Bucket", "mybucket")
	badForm.Set("X-Amz-Meta-Foo", "other")
	err := CheckPostPolicy(badForm, ppf)
	if err == nil {
		t.Fatalf("expected error when meta value misses required prefix, got nil")
	}
	if !strings.Contains(err.Error(), "Policy Condition failed") {
		t.Fatalf("expected 'Policy Condition failed' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "pfx-") {
		t.Fatalf("expected error to reference the required value prefix, got: %v", err)
	}
}

// TestCheckPostPolicy_ExactAndPrefixBothEnforced covers a field that is
// simultaneously covered by an exact-key condition and a prefix-stem
// condition. Both must be satisfied; exact-match alone does not let the
// field skip the stem's value-prefix check.
func TestCheckPostPolicy_ExactAndPrefixBothEnforced(t *testing.T) {
	ppf := buildParsedPolicy(t,
		`["eq","$bucket","mybucket"],`+
			`["eq","$x-amz-meta-tag","gold"],`+
			`["starts-with","$x-amz-meta-","lvl-"]`,
	)

	form := http.Header{}
	form.Set("Bucket", "mybucket")
	// Satisfies the exact eq condition but not the starts-with stem.
	form.Set("X-Amz-Meta-Tag", "gold")

	err := CheckPostPolicy(form, ppf)
	if err == nil {
		t.Fatalf("expected error: exact-match field must still satisfy overlapping prefix stem, got nil")
	}
	if !strings.Contains(err.Error(), "Policy Condition failed") {
		t.Fatalf("expected 'Policy Condition failed' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "lvl-") {
		t.Fatalf("expected error to reference the stem's value prefix, got: %v", err)
	}
}

// TestCheckPostPolicy_MultiplePrefixStemsAllEnforced covers a field that
// matches multiple starts-with prefix stems. All stems must hold; a value
// that satisfies one but not the other must be rejected.
func TestCheckPostPolicy_MultiplePrefixStemsAllEnforced(t *testing.T) {
	ppf := buildParsedPolicy(t,
		`["eq","$bucket","mybucket"],`+
			`["starts-with","$x-amz-meta-","pfx-"],`+
			`["starts-with","$x-amz-meta-color-","red-"]`,
	)

	// Satisfies the broader stem but not the color- stem's value prefix.
	form := http.Header{}
	form.Set("Bucket", "mybucket")
	form.Set("X-Amz-Meta-Color-Main", "pfx-green")

	err := CheckPostPolicy(form, ppf)
	if err == nil {
		t.Fatalf("expected error: field must satisfy every matching prefix stem, got nil")
	}
	if !strings.Contains(err.Error(), "Policy Condition failed") {
		t.Fatalf("expected 'Policy Condition failed' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "red-") {
		t.Fatalf("expected error to reference the failing value prefix, got: %v", err)
	}

	// A policy with compatible stems (broad allows anything, narrow
	// requires "red-") accepts a value honoring both.
	compatible := buildParsedPolicy(t,
		`["eq","$bucket","mybucket"],`+
			`["starts-with","$x-amz-meta-",""],`+
			`["starts-with","$x-amz-meta-color-","red-"]`,
	)
	okForm := http.Header{}
	okForm.Set("Bucket", "mybucket")
	okForm.Set("X-Amz-Meta-Color-Main", "red-main")
	if err := CheckPostPolicy(okForm, compatible); err != nil {
		t.Fatalf("expected no error when value satisfies both compatible stems, got: %v", err)
	}
}

// TestCheckPostPolicy_UnknownKeyErrorIncludesPolicyValue ensures the
// unknown-condition error surfaces the policy value in its [op, key, value]
// trailer so operators can tell which of several unknown keys failed.
func TestCheckPostPolicy_UnknownKeyErrorIncludesPolicyValue(t *testing.T) {
	ppf := buildParsedPolicy(t, `["eq","$foo","custom-value"]`)

	err := CheckPostPolicy(http.Header{}, ppf)
	if err == nil {
		t.Fatalf("expected error for unknown condition key, got nil")
	}
	if !strings.Contains(err.Error(), "custom-value") {
		t.Fatalf("expected error to include policy.Value 'custom-value', got: %v", err)
	}
	if !strings.Contains(err.Error(), "$foo") {
		t.Fatalf("expected error to include policy.Key '$foo', got: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown condition key") {
		t.Fatalf("expected 'unknown condition key' suffix, got: %v", err)
	}
}
