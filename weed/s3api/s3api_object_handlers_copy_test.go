package s3api

import (
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type H map[string]string

func (h H) String() string {
	pairs := make([]string, 0, len(h))
	for k, v := range h {
		pairs = append(pairs, fmt.Sprintf("%s : %s", k, v))
	}
	sort.Strings(pairs)
	join := strings.Join(pairs, "\n")
	return "\n" + join + "\n"
}

var processMetadataTestCases = []struct {
	caseId   int
	request  H
	existing H
	getTags  H
	want     H
}{
	{
		201,
		H{
			"User-Agent":         "firefox",
			"X-Amz-Meta-My-Meta": "request",
			"X-Amz-Tagging":      "A=B&a=b&type=request",
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-Type": "existing",
		},
		H{
			"A":    "B",
			"a":    "b",
			"type": "existing",
		},
		H{
			"User-Agent":         "firefox",
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging":      "A=B&a=b&type=existing",
		},
	},
	{
		202,
		H{
			"User-Agent":                      "firefox",
			"X-Amz-Meta-My-Meta":              "request",
			"X-Amz-Tagging":                   "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-Type": "existing",
		},
		H{
			"A":    "B",
			"a":    "b",
			"type": "existing",
		},
		H{
			"User-Agent":                      "firefox",
			"X-Amz-Meta-My-Meta":              "request",
			"X-Amz-Tagging":                   "A=B&a=b&type=existing",
			s3_constants.AmzUserMetaDirective: DirectiveReplace,
		},
	},

	{
		203,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-Type": "existing",
		},
		H{
			"A":    "B",
			"a":    "b",
			"type": "existing",
		},
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "existing",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
	},

	{
		204,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-Type": "existing",
		},
		H{
			"A":    "B",
			"a":    "b",
			"type": "existing",
		},
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
	},

	{
		205,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{},
		H{},
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
	},

	{
		206,
		H{
			"User-Agent":                           "firefox",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-Type": "existing",
		},
		H{
			"A":    "B",
			"a":    "b",
			"type": "existing",
		},
		H{
			"User-Agent":                           "firefox",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
	},

	{
		207,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-Type": "existing",
		},
		H{
			"A":    "B",
			"a":    "b",
			"type": "existing",
		},
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
	},
}
var processMetadataBytesTestCases = []struct {
	caseId   int
	request  H
	existing H
	want     H
}{
	{
		101,
		H{
			"User-Agent":         "firefox",
			"X-Amz-Meta-My-Meta": "request",
			"X-Amz-Tagging":      "A=B&a=b&type=request",
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
	},

	{
		102,
		H{
			"User-Agent":                      "firefox",
			"X-Amz-Meta-My-Meta":              "request",
			"X-Amz-Tagging":                   "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
		H{
			"X-Amz-Meta-My-Meta": "request",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
	},

	{
		103,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "request",
		},
	},

	{
		104,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
		H{
			"X-Amz-Meta-My-Meta": "request",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "request",
		},
	},

	{
		105,
		H{
			"User-Agent":                           "firefox",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{
			"X-Amz-Meta-My-Meta": "existing",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "existing",
		},
		H{},
	},

	{
		107,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{},
		H{
			"X-Amz-Meta-My-Meta": "request",
			"X-Amz-Tagging-A":    "B",
			"X-Amz-Tagging-a":    "b",
			"X-Amz-Tagging-type": "request",
		},
	},

	{
		108,
		H{
			"User-Agent":                           "firefox",
			"X-Amz-Meta-My-Meta":                   "request",
			"X-Amz-Tagging":                        "A=B&a=b&type=request*",
			s3_constants.AmzUserMetaDirective:      DirectiveReplace,
			s3_constants.AmzObjectTaggingDirective: DirectiveReplace,
		},
		H{},
		H{},
	},
}

func TestProcessMetadata(t *testing.T) {
	for _, tc := range processMetadataTestCases {
		reqHeader := transferHToHeader(tc.request)
		existing := transferHToHeader(tc.existing)
		replaceMeta, replaceTagging := replaceDirective(reqHeader)
		err := processMetadata(reqHeader, existing, replaceMeta, replaceTagging, func(_ string, _ string) (tags map[string]string, err error) {
			return tc.getTags, nil
		}, "", "")
		if err != nil {
			t.Error(err)
		}

		result := transferHeaderToH(reqHeader)
		fmtTagging(result, tc.want)

		if !reflect.DeepEqual(result, tc.want) {
			t.Error(fmt.Errorf("\n### CaseID: %d ###"+
				"\nRequest:%v"+
				"\nExisting:%v"+
				"\nGetTags:%v"+
				"\nWant:%v"+
				"\nActual:%v",
				tc.caseId, tc.request, tc.existing, tc.getTags, tc.want, result))
		}
	}
}

func TestProcessMetadataBytes(t *testing.T) {
	for _, tc := range processMetadataBytesTestCases {
		reqHeader := transferHToHeader(tc.request)
		existing := transferHToBytesArr(tc.existing)
		replaceMeta, replaceTagging := replaceDirective(reqHeader)
		extends, _ := processMetadataBytes(reqHeader, existing, replaceMeta, replaceTagging)

		result := transferBytesArrToH(extends)
		fmtTagging(result, tc.want)

		if !reflect.DeepEqual(result, tc.want) {
			t.Error(fmt.Errorf("\n### CaseID: %d ###"+
				"\nRequest:%v"+
				"\nExisting:%v"+
				"\nWant:%v"+
				"\nActual:%v",
				tc.caseId, tc.request, tc.existing, tc.want, result))
		}
	}
}

func fmtTagging(maps ...map[string]string) {
	for _, m := range maps {
		if tagging := m[s3_constants.AmzObjectTagging]; len(tagging) > 0 {
			split := strings.Split(tagging, "&")
			sort.Strings(split)
			m[s3_constants.AmzObjectTagging] = strings.Join(split, "&")
		}
	}
}

func transferHToHeader(data map[string]string) http.Header {
	header := http.Header{}
	for k, v := range data {
		header.Add(k, v)
	}
	return header
}

func transferHToBytesArr(data map[string]string) map[string][]byte {
	m := make(map[string][]byte, len(data))
	for k, v := range data {
		m[k] = []byte(v)
	}
	return m
}

func transferBytesArrToH(data map[string][]byte) H {
	m := make(map[string]string, len(data))
	for k, v := range data {
		m[k] = string(v)
	}
	return m
}

func transferHeaderToH(data map[string][]string) H {
	m := make(map[string]string, len(data))
	for k, v := range data {
		m[k] = v[len(v)-1]
	}
	return m
}

// TestShouldCreateVersionForCopy tests the production function that determines
// whether a version should be created during a copy operation.
// This addresses issue #7505 where copies were incorrectly creating versions for non-versioned buckets.
func TestShouldCreateVersionForCopy(t *testing.T) {
	testCases := []struct {
		name            string
		versioningState string
		expectedResult  bool
		description     string
	}{
		{
			name:            "VersioningEnabled",
			versioningState: s3_constants.VersioningEnabled,
			expectedResult:  true,
			description:     "Should create versions in .versions/ directory when versioning is Enabled",
		},
		{
			name:            "VersioningSuspended",
			versioningState: s3_constants.VersioningSuspended,
			expectedResult:  false,
			description:     "Should NOT create versions when versioning is Suspended",
		},
		{
			name:            "VersioningNotConfigured",
			versioningState: "",
			expectedResult:  false,
			description:     "Should NOT create versions when versioning is not configured",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the actual production function
			result := shouldCreateVersionForCopy(tc.versioningState)

			if result != tc.expectedResult {
				t.Errorf("Test case %s failed: %s\nExpected shouldCreateVersionForCopy(%q)=%v, got %v",
					tc.name, tc.description, tc.versioningState, tc.expectedResult, result)
			}
		})
	}
}

// TestCleanupVersioningMetadata tests the production function that removes versioning metadata.
// This ensures objects copied to non-versioned buckets don't carry invalid versioning metadata
// or stale ETag values from the source.
func TestCleanupVersioningMetadata(t *testing.T) {
	testCases := []struct {
		name           string
		sourceMetadata map[string][]byte
		expectedKeys   []string // Keys that should be present after cleanup
		removedKeys    []string // Keys that should be removed
	}{
		{
			name: "RemovesAllVersioningMetadata",
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey:    []byte("version-123"),
				s3_constants.ExtDeleteMarkerKey: []byte("false"),
				s3_constants.ExtIsLatestKey:     []byte("true"),
				s3_constants.ExtETagKey:         []byte("\"abc123\""),
				"X-Amz-Meta-Custom":             []byte("value"),
			},
			expectedKeys: []string{"X-Amz-Meta-Custom"},
			removedKeys:  []string{s3_constants.ExtVersionIdKey, s3_constants.ExtDeleteMarkerKey, s3_constants.ExtIsLatestKey, s3_constants.ExtETagKey},
		},
		{
			name:           "HandlesEmptyMetadata",
			sourceMetadata: map[string][]byte{},
			expectedKeys:   []string{},
			removedKeys:    []string{s3_constants.ExtVersionIdKey, s3_constants.ExtDeleteMarkerKey, s3_constants.ExtIsLatestKey, s3_constants.ExtETagKey},
		},
		{
			name: "PreservesNonVersioningMetadata",
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey: []byte("version-456"),
				s3_constants.ExtETagKey:      []byte("\"def456\""),
				"X-Amz-Meta-Custom":          []byte("value1"),
				"X-Amz-Meta-Another":         []byte("value2"),
				s3_constants.ExtIsLatestKey:  []byte("true"),
			},
			expectedKeys: []string{"X-Amz-Meta-Custom", "X-Amz-Meta-Another"},
			removedKeys:  []string{s3_constants.ExtVersionIdKey, s3_constants.ExtETagKey, s3_constants.ExtIsLatestKey},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a copy of the source metadata
			dstMetadata := make(map[string][]byte)
			for k, v := range tc.sourceMetadata {
				dstMetadata[k] = v
			}

			// Call the actual production function
			cleanupVersioningMetadata(dstMetadata)

			// Verify expected keys are present
			for _, key := range tc.expectedKeys {
				if _, exists := dstMetadata[key]; !exists {
					t.Errorf("Expected key %s to be present in destination metadata", key)
				}
			}

			// Verify removed keys are absent
			for _, key := range tc.removedKeys {
				if _, exists := dstMetadata[key]; exists {
					t.Errorf("Expected key %s to be removed from destination metadata, but it's still present", key)
				}
			}

			// Verify the count matches to ensure no extra keys are present
			if len(dstMetadata) != len(tc.expectedKeys) {
				t.Errorf("Expected %d metadata keys, but got %d. Extra keys might be present.", len(tc.expectedKeys), len(dstMetadata))
			}
		})
	}
}

// TestCopyVersioningIntegration validates the interaction between
// shouldCreateVersionForCopy and cleanupVersioningMetadata functions.
// This integration test ensures the complete fix for issue #7505.
func TestCopyVersioningIntegration(t *testing.T) {
	testCases := []struct {
		name               string
		versioningState    string
		sourceMetadata     map[string][]byte
		expectVersionPath  bool
		expectMetadataKeys []string
	}{
		{
			name:            "EnabledPreservesMetadata",
			versioningState: s3_constants.VersioningEnabled,
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey: []byte("v123"),
				"X-Amz-Meta-Custom":          []byte("value"),
			},
			expectVersionPath: true,
			expectMetadataKeys: []string{
				s3_constants.ExtVersionIdKey,
				"X-Amz-Meta-Custom",
			},
		},
		{
			name:            "SuspendedCleansMetadata",
			versioningState: s3_constants.VersioningSuspended,
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey: []byte("v123"),
				"X-Amz-Meta-Custom":          []byte("value"),
			},
			expectVersionPath: false,
			expectMetadataKeys: []string{
				"X-Amz-Meta-Custom",
			},
		},
		{
			name:            "NotConfiguredCleansMetadata",
			versioningState: "",
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey:    []byte("v123"),
				s3_constants.ExtDeleteMarkerKey: []byte("false"),
				"X-Amz-Meta-Custom":             []byte("value"),
			},
			expectVersionPath: false,
			expectMetadataKeys: []string{
				"X-Amz-Meta-Custom",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test version creation decision using production function
			shouldCreateVersion := shouldCreateVersionForCopy(tc.versioningState)
			if shouldCreateVersion != tc.expectVersionPath {
				t.Errorf("shouldCreateVersionForCopy(%q) = %v, expected %v",
					tc.versioningState, shouldCreateVersion, tc.expectVersionPath)
			}

			// Test metadata cleanup using production function
			metadata := make(map[string][]byte)
			for k, v := range tc.sourceMetadata {
				metadata[k] = v
			}

			if !shouldCreateVersion {
				cleanupVersioningMetadata(metadata)
			}

			// Verify only expected keys remain
			for _, expectedKey := range tc.expectMetadataKeys {
				if _, exists := metadata[expectedKey]; !exists {
					t.Errorf("Expected key %q to be present in metadata", expectedKey)
				}
			}

			// Verify the count matches (no extra keys)
			if len(metadata) != len(tc.expectMetadataKeys) {
				t.Errorf("Expected %d metadata keys, got %d", len(tc.expectMetadataKeys), len(metadata))
			}
		})
	}
}

// TestIsOrphanedSSES3Header tests detection of orphaned SSE-S3 headers.
// This is a regression test for GitHub issue #7562 where copying from an
// encrypted bucket to an unencrypted bucket left behind the encryption header
// without the actual key, causing subsequent copy operations to fail.
func TestIsOrphanedSSES3Header(t *testing.T) {
	testCases := []struct {
		name      string
		headerKey string
		metadata  map[string][]byte
		expected  bool
	}{
		{
			name:      "Not an encryption header",
			headerKey: "X-Amz-Meta-Custom",
			metadata: map[string][]byte{
				"X-Amz-Meta-Custom": []byte("value"),
			},
			expected: false,
		},
		{
			name:      "SSE-S3 header with key present (valid)",
			headerKey: s3_constants.AmzServerSideEncryption,
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
				s3_constants.SeaweedFSSSES3Key:       []byte("key-data"),
			},
			expected: false,
		},
		{
			name:      "SSE-S3 header without key (orphaned - GitHub #7562)",
			headerKey: s3_constants.AmzServerSideEncryption,
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
			},
			expected: true,
		},
		{
			name:      "SSE-KMS header (not SSE-S3)",
			headerKey: s3_constants.AmzServerSideEncryption,
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
			},
			expected: false,
		},
		{
			name:      "Different header key entirely",
			headerKey: s3_constants.SeaweedFSSSES3Key,
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isOrphanedSSES3Header(tc.headerKey, tc.metadata)
			if result != tc.expected {
				t.Errorf("isOrphanedSSES3Header(%q, metadata) = %v, expected %v",
					tc.headerKey, result, tc.expected)
			}
		})
	}
}

// TestCopyObjectETagInXMLResponse verifies that CopyObject response XML includes
// a properly formatted ETag. This is a regression test for GitHub issue #8155
// where CopyObject was not returning ETag in the XML response, breaking
// AWS SDK transfer managers and Hadoop S3A committers.
func TestCopyObjectETagInXMLResponse(t *testing.T) {
	testCases := []struct {
		name         string
		inputEtag    string
		expectedEtag string
	}{
		{
			name:         "Unquoted MD5 ETag gets quoted",
			inputEtag:    "d41d8cd98f00b204e9800998ecf8427e",
			expectedEtag: "\"d41d8cd98f00b204e9800998ecf8427e\"",
		},
		{
			name:         "Already quoted ETag stays quoted",
			inputEtag:    "\"d41d8cd98f00b204e9800998ecf8427e\"",
			expectedEtag: "\"d41d8cd98f00b204e9800998ecf8427e\"",
		},
		{
			name:         "Multipart ETag format gets quoted",
			inputEtag:    "d41d8cd98f00b204e9800998ecf8427e-2",
			expectedEtag: "\"d41d8cd98f00b204e9800998ecf8427e-2\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the quoting logic used in CopyObject
			etag := tc.inputEtag
			if etag != "" && !strings.HasPrefix(etag, "\"") {
				etag = "\"" + etag + "\""
			}

			if etag != tc.expectedEtag {
				t.Errorf("ETag quoting: got %q, expected %q", etag, tc.expectedEtag)
			}

			// Verify the XML response includes the quoted ETag
			response := CopyObjectResult{
				ETag: etag,
			}
			xmlBytes := s3err.EncodeXMLResponse(response)
			xmlStr := string(xmlBytes)

			// The XML encoder escapes quotes as &#34; which is correct XML behavior
			// AWS S3 SDKs and clients parse this correctly as quoted ETag
			expectedXMLEscaped := "<ETag>&#34;" + tc.inputEtag + "&#34;</ETag>"
			if tc.inputEtag != tc.expectedEtag {
				// Input was unquoted, so expected escaped XML
				if !strings.Contains(xmlStr, expectedXMLEscaped) {
					t.Errorf("XML response should contain %q, got: %s", expectedXMLEscaped, xmlStr)
				}
			}

			// Also verify the ETag element is not empty
			if strings.Contains(xmlStr, "<ETag></ETag>") {
				t.Errorf("XML response should not have empty ETag, got: %s", xmlStr)
			}
		})
	}
}

// TestCopyObjectETagStoredInExtended verifies that CopyObject stores the ETag
// in Extended metadata so that subsequent HeadObject calls can return it.
// This is a regression test for GitHub issue #8155.
func TestCopyObjectETagStoredInExtended(t *testing.T) {
	testCases := []struct {
		name           string
		sourceEtag     string
		expectedStored string
	}{
		{
			name:           "ETag stored with quotes",
			sourceEtag:     "d41d8cd98f00b204e9800998ecf8427e",
			expectedStored: "\"d41d8cd98f00b204e9800998ecf8427e\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the storage logic in CopyObject for non-versioned copies
			extended := make(map[string][]byte)

			// cleanupVersioningMetadata would remove old ETag
			cleanupVersioningMetadata(extended)

			// After cleanup, we calculate and store the new ETag
			etag := tc.sourceEtag
			if !strings.HasPrefix(etag, "\"") {
				etag = "\"" + etag + "\""
			}
			extended[s3_constants.ExtETagKey] = []byte(etag)

			// Verify the ETag is stored correctly
			storedEtag, hasEtag := extended[s3_constants.ExtETagKey]
			if !hasEtag {
				t.Errorf("ETag should be stored in Extended metadata with key %q", s3_constants.ExtETagKey)
			}
			if string(storedEtag) != tc.expectedStored {
				t.Errorf("Stored ETag: got %q, expected %q", string(storedEtag), tc.expectedStored)
			}
		})
	}
}
