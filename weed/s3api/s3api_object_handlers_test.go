package s3api

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// mockAccountManager implements AccountManager for testing
type mockAccountManager struct {
	accounts map[string]string
}

func (m *mockAccountManager) GetAccountNameById(id string) string {
	if name, exists := m.accounts[id]; exists {
		return name
	}
	return ""
}

func (m *mockAccountManager) GetAccountIdByEmail(email string) string {
	return ""
}

func TestNewListEntryOwnerDisplayName(t *testing.T) {
	// Create mock IAM with test accounts
	iam := &mockAccountManager{
		accounts: map[string]string{
			"testid":    "M. Tester",
			"userid123": "John Doe",
		},
	}

	// Create test entry with owner metadata
	entry := &filer_pb.Entry{
		Name: "test-object",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: 1024,
		},
		Extended: map[string][]byte{
			s3_constants.ExtAmzOwnerKey: []byte("testid"),
		},
	}

	// Test that display name is correctly looked up from IAM
	listEntry := newListEntry(entry, "", "dir", "test-object", "/buckets/test/", true, false, false, iam)

	assert.NotNil(t, listEntry.Owner, "Owner should be set when fetchOwner is true")
	assert.Equal(t, "testid", listEntry.Owner.ID, "Owner ID should match stored owner")
	assert.Equal(t, "M. Tester", listEntry.Owner.DisplayName, "Display name should be looked up from IAM")

	// Test with owner that doesn't exist in IAM (should fallback to ID)
	entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte("unknown-user")
	listEntry = newListEntry(entry, "", "dir", "test-object", "/buckets/test/", true, false, false, iam)

	assert.Equal(t, "unknown-user", listEntry.Owner.ID, "Owner ID should match stored owner")
	assert.Equal(t, "unknown-user", listEntry.Owner.DisplayName, "Display name should fallback to ID when not found in IAM")

	// Test with no owner metadata (should use anonymous)
	entry.Extended = make(map[string][]byte)
	listEntry = newListEntry(entry, "", "dir", "test-object", "/buckets/test/", true, false, false, iam)

	assert.Equal(t, s3_constants.AccountAnonymousId, listEntry.Owner.ID, "Should use anonymous ID when no owner metadata")
	assert.Equal(t, "anonymous", listEntry.Owner.DisplayName, "Should use anonymous display name when no owner metadata")

	// Test with fetchOwner false (should not set owner)
	listEntry = newListEntry(entry, "", "dir", "test-object", "/buckets/test/", false, false, false, iam)

	assert.Nil(t, listEntry.Owner, "Owner should not be set when fetchOwner is false")
}

func TestRemoveDuplicateSlashes(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedResult string
	}{
		{
			name:           "empty",
			path:           "",
			expectedResult: "",
		},
		{
			name:           "slash",
			path:           "/",
			expectedResult: "/",
		},
		{
			name:           "object",
			path:           "object",
			expectedResult: "object",
		},
		{
			name:           "correct path",
			path:           "/path/to/object",
			expectedResult: "/path/to/object",
		},
		{
			name:           "path with duplicates",
			path:           "///path//to/object//",
			expectedResult: "/path/to/object/",
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			obj := removeDuplicateSlashes(tst.path)
			assert.Equal(t, tst.expectedResult, obj)
		})
	}
}

func TestS3ApiServer_toFilerUrl(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{
			"simple",
			"/uploads/eaf10b3b-3b3a-4dcd-92a7-edf2a512276e/67b8b9bf-7cca-4cb6-9b34-22fcb4d6e27d/Bildschirmfoto 2022-09-19 um 21.38.37.png",
			"/uploads/eaf10b3b-3b3a-4dcd-92a7-edf2a512276e/67b8b9bf-7cca-4cb6-9b34-22fcb4d6e27d/Bildschirmfoto%202022-09-19%20um%2021.38.37.png",
		},
		{
			"double prefix",
			"//uploads/t.png",
			"/uploads/t.png",
		},
		{
			"triple prefix",
			"///uploads/t.png",
			"/uploads/t.png",
		},
		{
			"empty prefix",
			"uploads/t.png",
			"/uploads/t.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, urlEscapeObject(tt.args), "clean %v", tt.args)
		})
	}
}
