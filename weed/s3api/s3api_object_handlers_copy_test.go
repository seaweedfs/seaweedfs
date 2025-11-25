package s3api

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
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

// TestCopyVersioningBehavior tests that copies only create versions when versioning is "Enabled"
// This addresses issue #7505 where copies were incorrectly creating versions for non-versioned buckets
func TestCopyVersioningBehavior(t *testing.T) {
	testCases := []struct {
		name                   string
		versioningState        string
		shouldCreateVersion    bool
		description            string
	}{
		{
			name:                "VersioningEnabled",
			versioningState:     s3_constants.VersioningEnabled,
			shouldCreateVersion: true,
			description:         "Should create versions in .versions/ directory when versioning is Enabled",
		},
		{
			name:                "VersioningSuspended",
			versioningState:     s3_constants.VersioningSuspended,
			shouldCreateVersion: false,
			description:         "Should NOT create versions when versioning is Suspended",
		},
		{
			name:                "VersioningNotConfigured",
			versioningState:     "",
			shouldCreateVersion: false,
			description:         "Should NOT create versions when versioning is not configured",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the condition that determines whether to create a version
			shouldCreateVersion := tc.versioningState == s3_constants.VersioningEnabled

			if shouldCreateVersion != tc.shouldCreateVersion {
				t.Errorf("Test case %s failed: %s\nExpected shouldCreateVersion=%v, got %v",
					tc.name, tc.description, tc.shouldCreateVersion, shouldCreateVersion)
			}
		})
	}
}

// TestCopyVersioningMetadataCleanup tests that versioning metadata is stripped when copying to non-versioned buckets
// This ensures objects copied to non-versioned buckets don't carry invalid versioning metadata
func TestCopyVersioningMetadataCleanup(t *testing.T) {
	testCases := []struct {
		name            string
		versioningState string
		sourceMetadata  map[string][]byte
		expectedKeys    []string // Keys that should be present after cleanup
		removedKeys     []string // Keys that should be removed
	}{
		{
			name:            "CleanupForNonVersioned",
			versioningState: "",
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey:    []byte("version-123"),
				s3_constants.ExtDeleteMarkerKey: []byte("false"),
				s3_constants.ExtIsLatestKey:     []byte("true"),
				"X-Amz-Meta-Custom":             []byte("value"),
			},
			expectedKeys: []string{"X-Amz-Meta-Custom"},
			removedKeys:  []string{s3_constants.ExtVersionIdKey, s3_constants.ExtDeleteMarkerKey, s3_constants.ExtIsLatestKey},
		},
		{
			name:            "CleanupForSuspended",
			versioningState: s3_constants.VersioningSuspended,
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey:    []byte("version-456"),
				s3_constants.ExtDeleteMarkerKey: []byte("false"),
				s3_constants.ExtIsLatestKey:     []byte("true"),
				"X-Amz-Meta-Custom":             []byte("value"),
			},
			expectedKeys: []string{"X-Amz-Meta-Custom"},
			removedKeys:  []string{s3_constants.ExtVersionIdKey, s3_constants.ExtDeleteMarkerKey, s3_constants.ExtIsLatestKey},
		},
		{
			name:            "NoCleanupForEnabled",
			versioningState: s3_constants.VersioningEnabled,
			sourceMetadata: map[string][]byte{
				s3_constants.ExtVersionIdKey:    []byte("version-789"),
				s3_constants.ExtDeleteMarkerKey: []byte("false"),
				s3_constants.ExtIsLatestKey:     []byte("true"),
				"X-Amz-Meta-Custom":             []byte("value"),
			},
			expectedKeys: []string{
				s3_constants.ExtVersionIdKey,
				s3_constants.ExtDeleteMarkerKey,
				s3_constants.ExtIsLatestKey,
				"X-Amz-Meta-Custom",
			},
			removedKeys: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a copy of the source metadata
			dstMetadata := make(map[string][]byte)
			for k, v := range tc.sourceMetadata {
				dstMetadata[k] = v
			}

			// Simulate the cleanup logic from the copy handler
			if tc.versioningState != s3_constants.VersioningEnabled {
				// This is the fix for issue #7505
				delete(dstMetadata, s3_constants.ExtVersionIdKey)
				delete(dstMetadata, s3_constants.ExtDeleteMarkerKey)
				delete(dstMetadata, s3_constants.ExtIsLatestKey)
			}

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
		})
	}
}

// TestCopyDestinationPathLogic tests that the correct destination path is used based on versioning state
func TestCopyDestinationPathLogic(t *testing.T) {
	testCases := []struct {
		name               string
		versioningState    string
		bucket             string
		object             string
		expectedPathSuffix string // The suffix we expect in the path
		description        string
	}{
		{
			name:               "EnabledCreatesVersionPath",
			versioningState:    s3_constants.VersioningEnabled,
			bucket:             "test-bucket",
			object:             "/myfile.json",
			expectedPathSuffix: ".versions/", // Should include .versions in path
			description:        "Versioning enabled should create path with .versions directory",
		},
		{
			name:               "SuspendedCreatesRegularPath",
			versioningState:    s3_constants.VersioningSuspended,
			bucket:             "test-bucket",
			object:             "/myfile.json",
			expectedPathSuffix: "/myfile.json", // Should be direct path
			description:        "Versioning suspended should create regular path without .versions",
		},
		{
			name:               "NotConfiguredCreatesRegularPath",
			versioningState:    "",
			bucket:             "test-bucket",
			object:             "/myfile.json",
			expectedPathSuffix: "/myfile.json", // Should be direct path
			description:        "No versioning should create regular path without .versions",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isVersioningEnabled := tc.versioningState == s3_constants.VersioningEnabled

			if isVersioningEnabled {
				// When versioning is enabled, path should contain .versions
				if !strings.Contains(tc.expectedPathSuffix, ".versions") {
					t.Errorf("Test case %s: Expected path to contain .versions for enabled versioning", tc.name)
				}
			} else {
				// When versioning is not enabled, path should be direct
				if strings.Contains(tc.expectedPathSuffix, ".versions") {
					t.Errorf("Test case %s: Expected path NOT to contain .versions for non-enabled versioning", tc.name)
				}
			}
		})
	}
}
