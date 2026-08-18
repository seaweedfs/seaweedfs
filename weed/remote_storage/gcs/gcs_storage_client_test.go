package gcs

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/require"
)

func TestGCSRemoteStorageClientImplementsInterface(t *testing.T) {
	var _ remote_storage.RemoteStorageClient = (*gcsRemoteStorageClient)(nil)
}

func TestGCSErrRemoteObjectNotFoundIsAccessible(t *testing.T) {
	require.Error(t, remote_storage.ErrRemoteObjectNotFound)
	require.Equal(t, "remote object not found", remote_storage.ErrRemoteObjectNotFound.Error())
}

// TestMakeWithHTTPClientAllowedTypes covers the restriction a caller applies to
// credentials it does not control: a federated document never reaches the SDK,
// while an unrestricted caller keeps loading whatever the operator configured.
func TestMakeWithHTTPClientAllowedTypes(t *testing.T) {
	federated := `{"type":"external_account","audience":"a","subject_token_type":"t","token_url":"http://127.0.0.1:9/v1/token","credential_source":{"url":"http://169.254.169.254/"}}`
	conf := &remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: federated}

	_, err := MakeWithHTTPClient(conf, nil, StaticKeyCredentialTypes...)
	require.ErrorContains(t, err, `"external_account" is not accepted`)

	_, err = MakeWithHTTPClient(conf, nil)
	require.NoError(t, err)
}

func TestParseInlineCredentials(t *testing.T) {
	credType, tokenURL, err := ParseInlineCredentials(`{"type":"service_account"}`)
	require.NoError(t, err)
	require.Equal(t, "service_account", credType)
	require.Equal(t, defaultTokenURL, tokenURL)

	_, tokenURL, err = ParseInlineCredentials(`{"type":"service_account","token_uri":"https://example.com/t"}`)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/t", tokenURL)

	_, _, err = ParseInlineCredentials(`not json`)
	require.Error(t, err)
}
