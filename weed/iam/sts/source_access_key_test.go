package sts

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSourceKeyTestSTS(t *testing.T) *STSService {
	t.Helper()
	s := NewSTSService()
	require.NoError(t, s.Initialize(&STSConfig{
		TokenDuration:    FlexibleDuration{Duration: time.Hour},
		MaxSessionLength: FlexibleDuration{Duration: 12 * time.Hour},
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-at-least-32-bytes-long-for-security"),
	}))
	return s
}

// A session minted for a caller's access key carries that key back out of its
// token; a session minted without one carries none.
func TestSessionCarriesItsSourceAccessKey(t *testing.T) {
	s := newSourceKeyTestSTS(t)
	resp, err := s.issueSession("arn:aws:iam::role/IcebergTableAccess", "", "iceberg-alice-ns-t-kabc", "", nil,
		"iceberg-catalog", "alice", func(c *STSSessionClaims) { c.WithSourceAccessKeyId("AKIASOURCE") })
	require.NoError(t, err)
	info, err := s.ValidateSessionToken(context.Background(), resp.Credentials.SessionToken)
	require.NoError(t, err)
	assert.Equal(t, "AKIASOURCE", info.SourceAccessKeyId)
	assert.Equal(t, "alice", info.ExternalUserId)
	assert.Equal(t, "iceberg-catalog", info.IdentityProvider)

	resp, err = s.issueSession("arn:aws:iam::role/IcebergTableAccess", "", "iceberg-alice-ns-t", "", nil, "iceberg-catalog", "alice")
	require.NoError(t, err)
	info, err = s.ValidateSessionToken(context.Background(), resp.Credentials.SessionToken)
	require.NoError(t, err)
	assert.Equal(t, "", info.SourceAccessKeyId)
}
