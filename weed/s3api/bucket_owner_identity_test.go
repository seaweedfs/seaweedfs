package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownerIdentityIAM loads an account-less identity and one scoped to an explicit
// account, the two shapes a bucket owner name can resolve through.
func ownerIdentityIAM(t *testing.T) *IdentityAccessManagement {
	t.Helper()
	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "test", Actions: []string{"Read", "Write"}},
			{
				Name:    "scoped",
				Actions: []string{"Read", "Write"},
				Account: &iam_pb.Account{Id: "100000000001", DisplayName: "Scoped"},
			},
		},
	}))
	return iam
}

// A bucket created outside the S3 API — the admin UI or weed shell, which record
// the owner as an identity and never write the account id the S3 API stores — is
// owned by that identity, not by the default admin account. Getting this wrong
// stamps every object uploaded to the bucket with the admin account, since the
// default BucketOwnerEnforced ownership hands objects to the bucket owner.
func TestBucketOwnerFromIdentityId(t *testing.T) {
	iam := ownerIdentityIAM(t)

	for _, tc := range []struct {
		name          string
		identityId    string
		expectOwnerId string
	}{
		{"account-less identity", "test", "test"},
		{"identity scoped to an account", "scoped", "100000000001"},
		{"unknown identity", "ghost", AccountAdmin.Id},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := &filer_pb.Entry{
				Name:     "bucket",
				Extended: map[string][]byte{s3_constants.AmzIdentityId: []byte(tc.identityId)},
			}

			metadata := buildBucketMetadata(iam, entry)
			require.NotNil(t, metadata.Owner)
			assert.Equal(t, tc.expectOwnerId, *metadata.Owner.ID)

			s3a := &S3ApiServer{iam: iam}
			config := s3a.newBucketConfigFromEntry("bucket", entry)
			assert.Equal(t, tc.identityId, config.IdentityId)
			if tc.expectOwnerId != AccountAdmin.Id {
				assert.Equal(t, tc.expectOwnerId, config.Owner,
					"the bucket config owner must match the metadata owner")
			} else {
				assert.Empty(t, config.Owner, "an unresolvable owner leaves the config unowned")
			}
		})
	}
}

// The account id recorded by the S3 API stays authoritative when both are present.
func TestBucketOwnerPrefersAccountId(t *testing.T) {
	iam := ownerIdentityIAM(t)

	entry := &filer_pb.Entry{
		Name: "bucket",
		Extended: map[string][]byte{
			s3_constants.AmzIdentityId:  []byte("scoped"),
			s3_constants.ExtAmzOwnerKey: []byte("test"),
		},
	}

	metadata := buildBucketMetadata(iam, entry)
	require.NotNil(t, metadata.Owner)
	assert.Equal(t, "test", *metadata.Owner.ID)
}
