package s3tables

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errFilerReached = errors.New("filer reached")

// recordingFilerClient reports whether the handler reached the filer. The
// CreateTableBucket handler authorizes before touching the filer, so "filer
// reached" is a proxy for "authorization passed".
type recordingFilerClient struct {
	called bool
}

func (c *recordingFilerClient) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	c.called = true
	return errFilerReached
}

// The Manager path (used by the Iceberg catalog) enforces authorization for
// authenticated callers and only falls open for trusted/zero-config access.
func TestManagerCreateTableBucketAuthorization(t *testing.T) {
	lowPriv := &testIdentity{
		Name:    "alice",
		Account: &testIdentityAccount{Id: s3_constants.AccountAdminId},
		Actions: []string{"Read"},
	}

	cases := []struct {
		name         string
		defaultAllow bool
		trusted      bool
		ctx          context.Context
		identity     string
		wantFiler    bool // true => authorization passed (filer reached)
	}{
		{
			name:         "authenticated identity struct is enforced",
			defaultAllow: true,
			ctx:          s3_constants.SetIdentityInContext(context.Background(), lowPriv),
			identity:     "alice",
			wantFiler:    false,
		},
		{
			name:         "secured manager denies a name without struct",
			defaultAllow: false,
			ctx:          context.Background(),
			identity:     "alice",
			wantFiler:    false,
		},
		{
			name:         "untrusted name without struct is enforced",
			defaultAllow: true,
			ctx:          context.Background(),
			identity:     "alice",
			wantFiler:    false,
		},
		{
			name:         "trusted manager allows a name without struct",
			defaultAllow: true,
			trusted:      true,
			ctx:          context.Background(),
			identity:     "alice",
			wantFiler:    true,
		},
		{
			name:         "admin principal is allowed",
			defaultAllow: false,
			ctx:          context.Background(),
			identity:     s3_constants.AccountAdminId,
			wantFiler:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager()
			m.SetDefaultAllow(tc.defaultAllow)
			m.SetTrusted(tc.trusted)
			fc := &recordingFilerClient{}
			err := m.Execute(tc.ctx, fc, "CreateTableBucket", &CreateTableBucketRequest{Name: "testbucket"}, nil, tc.identity)
			require.Error(t, err) // denied (403) or the filer sentinel (reached); never nil here
			assert.Equal(t, tc.wantFiler, fc.called, "filer reached should equal authorization passed")
			if !tc.wantFiler {
				var s3Err *S3TablesError
				require.ErrorAs(t, err, &s3Err)
				assert.Equal(t, ErrCodeAccessDenied, s3Err.Type)
			}
		})
	}
}
