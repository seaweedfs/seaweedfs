package iamapi

// Targeted-write regression tests: single-identity IAM actions must write only
// the identity they changed through the credential manager, not every identity
// via PutS3ApiConfiguration/SaveConfiguration.
// See https://github.com/seaweedfs/seaweedfs/issues/11865.

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// targetedRecordingStore wraps a memory store and counts the write methods
// the dispatcher used, so a test can assert the single-identity path was
// taken. It embeds the concrete store (rather than the CredentialStore
// interface) so optional interfaces like InlinePolicyStore stay visible
// through the wrapper.
type targetedRecordingStore struct {
	*memory.MemoryStore
	saves            int
	createAccessKeys int
	deleteAccessKeys int
	updateUsers      int
	deleteUsers      int
	updateGroups     int
}

func (r *targetedRecordingStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	r.saves++
	return r.MemoryStore.SaveConfiguration(ctx, config)
}

func (r *targetedRecordingStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	r.createAccessKeys++
	return r.MemoryStore.CreateAccessKey(ctx, username, cred)
}

func (r *targetedRecordingStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	r.deleteAccessKeys++
	return r.MemoryStore.DeleteAccessKey(ctx, username, accessKey)
}

func (r *targetedRecordingStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	r.updateUsers++
	return r.MemoryStore.UpdateUser(ctx, username, identity)
}

func (r *targetedRecordingStore) DeleteUser(ctx context.Context, username string) error {
	r.deleteUsers++
	return r.MemoryStore.DeleteUser(ctx, username)
}

func (r *targetedRecordingStore) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	r.updateGroups++
	return r.MemoryStore.UpdateGroup(ctx, group)
}

func targetedIamApiServer() (*IamApiServer, *credential.CredentialManager, *countingConfigSaver, *targetedRecordingStore) {
	srv, cm, counter := iamApiServerWithCredentialManager()
	rec := &targetedRecordingStore{MemoryStore: cm.Store.(*memory.MemoryStore)}
	cm.Store = rec
	return srv, cm, counter, rec
}

const targetedTestPolicy = `{
	"Version": "2012-10-17",
	"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::bench/*"]}]
}`

func TestIamApiCreateAccessKeyTargeted(t *testing.T) {
	srv, cm, counter, rec := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}))
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "bob"}))

	req, _ := iam.New(session.New()).CreateAccessKeyRequest(&iam.CreateAccessKeyInput{UserName: aws.String("alice")})
	_ = req.Build()
	out := CreateAccessKeyResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "CreateAccessKey must not PutS3ApiConfiguration")
	assert.Equal(t, 0, rec.saves)
	assert.Equal(t, 1, rec.createAccessKeys)

	ident, err := cm.GetUser(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, ident.Credentials, 1)
	assert.Equal(t, *out.CreateAccessKeyResult.AccessKey.AccessKeyId, ident.Credentials[0].AccessKey)
}

// CreateAccessKey for a user the store does not know still needs the full save:
// the handler creates the identity in the config, so there is no existing
// record to write a key into.
func TestIamApiCreateAccessKeyUnknownUserFallsBack(t *testing.T) {
	srv, cm, counter, _ := targetedIamApiServer()
	ctx := context.Background()

	req, _ := iam.New(session.New()).CreateAccessKeyRequest(&iam.CreateAccessKeyInput{UserName: aws.String("ghost")})
	_ = req.Build()
	out := CreateAccessKeyResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 1, counter.putCalled)
	ident, err := cm.GetUser(ctx, "ghost")
	require.NoError(t, err)
	assert.Len(t, ident.Credentials, 1)
}

func TestIamApiDeleteAccessKeyTargeted(t *testing.T) {
	srv, cm, counter, rec := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{
		Name:        "alice",
		Credentials: []*iam_pb.Credential{{AccessKey: "AKIATESTFAKEKEY00001", SecretKey: "testsecretfake"}},
	}))

	req, _ := iam.New(session.New()).DeleteAccessKeyRequest(&iam.DeleteAccessKeyInput{
		UserName:    aws.String("alice"),
		AccessKeyId: aws.String("AKIATESTFAKEKEY00001"),
	})
	_ = req.Build()
	out := DeleteAccessKeyResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "DeleteAccessKey must not PutS3ApiConfiguration")
	assert.Equal(t, 1, rec.deleteAccessKeys)

	ident, err := cm.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.Empty(t, ident.Credentials)

	// Deleting a key that does not exist still answers 200 and stays targeted.
	req, _ = iam.New(session.New()).DeleteAccessKeyRequest(&iam.DeleteAccessKeyInput{
		UserName:    aws.String("alice"),
		AccessKeyId: aws.String("AKIATESTFAKEKEY99999"),
	})
	_ = req.Build()
	resp, err = executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, 0, counter.putCalled)
}

func TestIamApiUpdateAccessKeyTargeted(t *testing.T) {
	srv, cm, counter, rec := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{
		Name:        "alice",
		Credentials: []*iam_pb.Credential{{AccessKey: "AKIATESTFAKEKEY00001", SecretKey: "testsecretfake", Status: "Active"}},
	}))

	req, _ := iam.New(session.New()).UpdateAccessKeyRequest(&iam.UpdateAccessKeyInput{
		UserName:    aws.String("alice"),
		AccessKeyId: aws.String("AKIATESTFAKEKEY00001"),
		Status:      aws.String(iam.StatusTypeInactive),
	})
	_ = req.Build()
	out := UpdateAccessKeyResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "UpdateAccessKey must not PutS3ApiConfiguration")
	assert.Equal(t, 1, rec.updateUsers)

	ident, err := cm.GetUser(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, ident.Credentials, 1)
	assert.Equal(t, iam.StatusTypeInactive, ident.Credentials[0].Status)
}

func TestIamApiPutUserPolicyTargeted(t *testing.T) {
	srv, cm, counter, rec := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}))
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "bob"}))

	req, _ := iam.New(session.New()).PutUserPolicyRequest(&iam.PutUserPolicyInput{
		UserName:       aws.String("alice"),
		PolicyName:     aws.String("read-bench"),
		PolicyDocument: aws.String(targetedTestPolicy),
	})
	_ = req.Build()
	out := PutUserPolicyResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "PutUserPolicy must not PutS3ApiConfiguration")
	assert.Equal(t, 0, rec.saves)
	assert.Equal(t, 1, rec.updateUsers)

	ident, err := cm.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.NotEmpty(t, ident.Actions, "recomputed Actions must be persisted for the one identity")
}

func TestIamApiDeleteUserPolicyTargeted(t *testing.T) {
	srv, cm, counter, rec := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}))

	putReq, _ := iam.New(session.New()).PutUserPolicyRequest(&iam.PutUserPolicyInput{
		UserName:       aws.String("alice"),
		PolicyName:     aws.String("read-bench"),
		PolicyDocument: aws.String(targetedTestPolicy),
	})
	_ = putReq.Build()
	putOut := PutUserPolicyResponse{}
	resp, err := executeRequestWith(srv, putReq.HTTPRequest, &putOut)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	delReq, _ := iam.New(session.New()).DeleteUserPolicyRequest(&iam.DeleteUserPolicyInput{
		UserName:   aws.String("alice"),
		PolicyName: aws.String("read-bench"),
	})
	_ = delReq.Build()
	delOut := DeleteUserPolicyResponse{}
	resp, err = executeRequestWith(srv, delReq.HTTPRequest, &delOut)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "DeleteUserPolicy must not PutS3ApiConfiguration")
	assert.Equal(t, 2, rec.updateUsers)

	policies := Policies{}
	require.NoError(t, srv.s3ApiConfig.GetPolicies(&policies))
	assert.Empty(t, policies.InlinePolicies["alice"])
}

func TestIamApiUpdateUserRenameTargeted(t *testing.T) {
	srv, cm, counter, _ := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}))
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "bob"}))
	require.NoError(t, cm.CreateGroup(ctx, &iam_pb.Group{Name: "devs", Members: []string{"alice", "bob"}}))

	req, _ := iam.New(session.New()).UpdateUserRequest(&iam.UpdateUserInput{
		UserName:    aws.String("alice"),
		NewUserName: aws.String("alice-renamed"),
	})
	_ = req.Build()
	out := UpdateUserResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "UpdateUser rename must not PutS3ApiConfiguration")

	_, err = cm.GetUser(ctx, "alice")
	require.ErrorIs(t, err, credential.ErrUserNotFound)
	_, err = cm.GetUser(ctx, "alice-renamed")
	require.NoError(t, err)

	g, err := cm.GetGroup(ctx, "devs")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice-renamed", "bob"}, g.Members)
}

func TestIamApiDeleteUserTargeted(t *testing.T) {
	srv, cm, counter, rec := targetedIamApiServer()
	ctx := context.Background()
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}))
	require.NoError(t, cm.CreateUser(ctx, &iam_pb.Identity{Name: "bob"}))
	require.NoError(t, cm.CreateGroup(ctx, &iam_pb.Group{Name: "devs", Members: []string{"alice", "bob"}}))

	req, _ := iam.New(session.New()).DeleteUserRequest(&iam.DeleteUserInput{UserName: aws.String("alice")})
	_ = req.Build()
	out := DeleteUserResponse{}
	resp, err := executeRequestWith(srv, req.HTTPRequest, &out)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	assert.Equal(t, 0, counter.putCalled, "DeleteUser must not PutS3ApiConfiguration")
	assert.Equal(t, 1, rec.deleteUsers)
	assert.Equal(t, 1, rec.updateGroups)

	_, err = cm.GetUser(ctx, "alice")
	require.ErrorIs(t, err, credential.ErrUserNotFound)
	g, err := cm.GetGroup(ctx, "devs")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bob"}, g.Members)
	_, err = cm.GetUser(ctx, "bob")
	require.NoError(t, err, "other identities must be untouched")
}
