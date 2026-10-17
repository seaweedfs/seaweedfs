package s3api

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PutIdentity pushes carry the persisted identity proto, which records the
// inline policy flattened into Actions but not its runtime policy name. The
// upsert must keep the identity on the attached-policy route; otherwise its
// coarse "Write:bucket" action authorizes deletes the policy never granted.
// The push must also install the policy document itself: the inline policy
// here is never loaded by a full reload, so only the push can supply it.
func TestUpsertIdentityKeepsInlinePolicyAuthorization(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{
		Name:        "writer",
		Credentials: []*iam_pb.Credential{{AccessKey: "AKW", SecretKey: "secret"}},
		Actions:     []string{"Read:bucket", "Write:bucket", "List:bucket"},
	}})
	ctx := context.Background()

	var doc policy_engine.PolicyDocument
	require.NoError(t, doc.UnmarshalJSON([]byte(`{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":["s3:GetObject","s3:PutObject","s3:ListBucket"],
			"Resource":["arn:aws:s3:::bucket","arn:aws:s3:::bucket/*"]
		}]
	}`)))
	require.NoError(t, s3a.iam.credentialManager.PutUserInlinePolicy(ctx, "writer", "writer", doc))

	pushed, err := s3a.iam.credentialManager.GetUser(ctx, "writer")
	require.NoError(t, err)
	require.NotContains(t, pushed.PolicyNames, inlinePolicyRuntimeName("writer", "writer"),
		"persisted protos do not carry runtime inline policy names")
	require.NoError(t, s3a.iam.UpsertIdentity(pushed))

	ident := s3a.iam.lookupByIdentityName("writer")
	require.NotNil(t, ident)
	require.Contains(t, ident.PolicyNames, inlinePolicyRuntimeName("writer", "writer"))

	_, err = s3a.iam.GetPolicy(inlinePolicyRuntimeName("writer", "writer"))
	require.NoError(t, err, "the push must install the inline policy document, not just its name")

	req, err := http.NewRequest(http.MethodGet, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	assert.Equal(t, s3err.ErrNone,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_READ, "bucket", "obj"),
		"an allowed request must succeed through the pushed inline policy")

	req, err = http.NewRequest(http.MethodDelete, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_WRITE, "bucket", "obj"),
		"an identity pushed without its inline policy must not fall back to legacy Actions")
}

// A pushed identity must also refresh a cached inline policy document that has
// since been replaced in the store; otherwise the stale allow keeps
// authorizing requests the new policy revoked.
func TestUpsertIdentityRefreshesInlinePolicyDocument(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{
		Name:        "writer",
		Credentials: []*iam_pb.Credential{{AccessKey: "AKW", SecretKey: "secret"}},
		Actions:     []string{"Read:bucket", "Write:bucket", "List:bucket"},
	}})
	ctx := context.Background()

	var doc policy_engine.PolicyDocument
	require.NoError(t, doc.UnmarshalJSON([]byte(`{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]
	}`)))
	require.NoError(t, s3a.iam.credentialManager.PutUserInlinePolicy(ctx, "writer", "writer", doc))
	require.NoError(t, s3a.iam.LoadS3ApiConfigurationFromCredentialManager())

	ident := s3a.iam.lookupByIdentityName("writer")
	require.NotNil(t, ident)
	req, err := http.NewRequest(http.MethodGet, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	require.Equal(t, s3err.ErrNone,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_READ, "bucket", "obj"))

	require.NoError(t, doc.UnmarshalJSON([]byte(`{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]
	}`)))
	require.NoError(t, s3a.iam.credentialManager.PutUserInlinePolicy(ctx, "writer", "writer", doc))

	pushed, err := s3a.iam.credentialManager.GetUser(ctx, "writer")
	require.NoError(t, err)
	require.NoError(t, s3a.iam.UpsertIdentity(pushed))

	ident = s3a.iam.lookupByIdentityName("writer")
	require.NotNil(t, ident)
	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_READ, "bucket", "obj"),
		"a push must replace the stale cached document, not keep the old allow")
}

// The same applies to PutGroup pushes: the persisted group proto lacks the
// group's runtime inline policy names, and the inline document is never
// loaded by a full reload here, so only the push can supply it.
func TestPutGroupKeepsInlinePolicyAuthorization(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{Name: "alice"}})
	ctx := context.Background()

	require.NoError(t, s3a.iam.credentialManager.CreateGroup(ctx,
		&iam_pb.Group{Name: "devs", Members: []string{"alice"}}))
	var doc policy_engine.PolicyDocument
	require.NoError(t, doc.UnmarshalJSON([]byte(`{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]
	}`)))
	require.NoError(t, s3a.iam.credentialManager.PutGroupInlinePolicy(ctx, "devs", "p", doc))

	pushed, err := s3a.iam.credentialManager.GetGroup(ctx, "devs")
	require.NoError(t, err)
	require.NoError(t, s3a.iam.PutGroup(pushed))

	s3a.iam.m.RLock()
	require.Contains(t, s3a.iam.groups["devs"].PolicyNames, inlineGroupPolicyRuntimeName("devs", "p"))
	s3a.iam.m.RUnlock()

	_, err = s3a.iam.GetPolicy(inlineGroupPolicyRuntimeName("devs", "p"))
	require.NoError(t, err, "the push must install the group's inline policy document")

	ident := s3a.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)
	req, err := http.NewRequest(http.MethodGet, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	assert.Equal(t, s3err.ErrNone,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_READ, "bucket", "obj"),
		"a pushed group must keep its inline policy grant active")
}

// pausingInlineStore pauses only the first user and first group inline policy
// Get, after capturing the document, so a test can interleave a newer write
// between a push's store read and the install of what it read. Later calls
// proceed immediately.
type pausingInlineStore struct {
	credential.CredentialStore

	userGets     int32
	userPaused   chan struct{}
	userRelease  chan struct{}
	groupGets    int32
	groupPaused  chan struct{}
	groupRelease chan struct{}
}

func (s *pausingInlineStore) userPolicies() credential.InlinePolicyStore {
	return s.CredentialStore.(credential.InlinePolicyStore)
}

func (s *pausingInlineStore) groupPolicies() credential.GroupInlinePolicyStore {
	return s.CredentialStore.(credential.GroupInlinePolicyStore)
}

func (s *pausingInlineStore) PutUserInlinePolicy(ctx context.Context, userName, policyName string, doc policy_engine.PolicyDocument) error {
	return s.userPolicies().PutUserInlinePolicy(ctx, userName, policyName, doc)
}

func (s *pausingInlineStore) GetUserInlinePolicy(ctx context.Context, userName, policyName string) (*policy_engine.PolicyDocument, error) {
	doc, err := s.userPolicies().GetUserInlinePolicy(ctx, userName, policyName)
	if err != nil {
		return nil, err
	}
	if atomic.AddInt32(&s.userGets, 1) == 1 {
		close(s.userPaused)
		<-s.userRelease
	}
	return doc, nil
}

func (s *pausingInlineStore) DeleteUserInlinePolicy(ctx context.Context, userName, policyName string) error {
	return s.userPolicies().DeleteUserInlinePolicy(ctx, userName, policyName)
}

func (s *pausingInlineStore) ListUserInlinePolicies(ctx context.Context, userName string) ([]string, error) {
	return s.userPolicies().ListUserInlinePolicies(ctx, userName)
}

func (s *pausingInlineStore) PutGroupInlinePolicy(ctx context.Context, groupName, policyName string, doc policy_engine.PolicyDocument) error {
	return s.groupPolicies().PutGroupInlinePolicy(ctx, groupName, policyName, doc)
}

func (s *pausingInlineStore) GetGroupInlinePolicy(ctx context.Context, groupName, policyName string) (*policy_engine.PolicyDocument, error) {
	doc, err := s.groupPolicies().GetGroupInlinePolicy(ctx, groupName, policyName)
	if err != nil {
		return nil, err
	}
	if atomic.AddInt32(&s.groupGets, 1) == 1 {
		close(s.groupPaused)
		<-s.groupRelease
	}
	return doc, nil
}

func (s *pausingInlineStore) DeleteGroupInlinePolicy(ctx context.Context, groupName, policyName string) error {
	return s.groupPolicies().DeleteGroupInlinePolicy(ctx, groupName, policyName)
}

func (s *pausingInlineStore) ListGroupInlinePolicies(ctx context.Context, groupName string) ([]string, error) {
	return s.groupPolicies().ListGroupInlinePolicies(ctx, groupName)
}

// A push paused after reading a permissive inline policy must not overwrite a
// tighter document a later push already installed: once the pause lifts, the
// stale read may still land, but the newer push must be the state that wins.
func TestUpsertIdentityRaceKeepsNewerInlinePolicy(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{
		Name:        "writer",
		Credentials: []*iam_pb.Credential{{AccessKey: "AKW", SecretKey: "secret"}},
	}})
	ctx := context.Background()

	pauser := &pausingInlineStore{
		CredentialStore: s3a.iam.credentialManager.GetStore(),
		userPaused:      make(chan struct{}),
		userRelease:     make(chan struct{}),
	}
	s3a.iam.credentialManager.Store = pauser

	var docV1 policy_engine.PolicyDocument
	require.NoError(t, docV1.UnmarshalJSON([]byte(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)))
	require.NoError(t, s3a.iam.credentialManager.PutUserInlinePolicy(ctx, "writer", "p", docV1))

	pushed, err := s3a.iam.credentialManager.GetUser(ctx, "writer")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, s3a.iam.UpsertIdentity(pushed))
	}()

	select {
	case <-pauser.userPaused:
	case <-time.After(5 * time.Second):
		t.Fatal("first push did not reach the inline policy read")
	}

	// The administrator tightens the policy; its push must be the surviving one.
	var docV2 policy_engine.PolicyDocument
	require.NoError(t, docV2.UnmarshalJSON([]byte(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`)))
	require.NoError(t, s3a.iam.credentialManager.PutUserInlinePolicy(ctx, "writer", "p", docV2))

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, s3a.iam.UpsertIdentity(pushed))
	}()

	// Give the second push a moment to install the newer document; with the
	// ordering fix it instead waits for the first push and this poll expires.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p, err := s3a.iam.GetPolicy(inlinePolicyRuntimeName("writer", "p")); err == nil && strings.Contains(p.Content, "s3:ListBucket") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(pauser.userRelease)
	wg.Wait()

	installed, err := s3a.iam.GetPolicy(inlinePolicyRuntimeName("writer", "p"))
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`,
		installed.Content, "the newer pushed document must win over the stale read")

	ident := s3a.iam.lookupByIdentityName("writer")
	require.NotNil(t, ident)
	req, err := http.NewRequest(http.MethodGet, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_READ, "bucket", "obj"),
		"a stale push must not restore a grant the newer policy revoked")
}

// Same ordering guarantee for group pushes: a paused PutGroup must not
// overwrite a tighter inline document installed by a later push.
func TestPutGroupRaceKeepsNewerInlinePolicy(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{Name: "alice"}})
	ctx := context.Background()

	pauser := &pausingInlineStore{
		CredentialStore: s3a.iam.credentialManager.GetStore(),
		groupPaused:     make(chan struct{}),
		groupRelease:    make(chan struct{}),
	}
	s3a.iam.credentialManager.Store = pauser

	require.NoError(t, s3a.iam.credentialManager.CreateGroup(ctx,
		&iam_pb.Group{Name: "devs", Members: []string{"alice"}}))
	var docV1 policy_engine.PolicyDocument
	require.NoError(t, docV1.UnmarshalJSON([]byte(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)))
	require.NoError(t, s3a.iam.credentialManager.PutGroupInlinePolicy(ctx, "devs", "p", docV1))

	pushed, err := s3a.iam.credentialManager.GetGroup(ctx, "devs")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, s3a.iam.PutGroup(pushed))
	}()

	select {
	case <-pauser.groupPaused:
	case <-time.After(5 * time.Second):
		t.Fatal("first group push did not reach the inline policy read")
	}

	var docV2 policy_engine.PolicyDocument
	require.NoError(t, docV2.UnmarshalJSON([]byte(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`)))
	require.NoError(t, s3a.iam.credentialManager.PutGroupInlinePolicy(ctx, "devs", "p", docV2))

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, s3a.iam.PutGroup(pushed))
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p, err := s3a.iam.GetPolicy(inlineGroupPolicyRuntimeName("devs", "p")); err == nil && strings.Contains(p.Content, "s3:ListBucket") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(pauser.groupRelease)
	wg.Wait()

	installed, err := s3a.iam.GetPolicy(inlineGroupPolicyRuntimeName("devs", "p"))
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`,
		installed.Content, "the newer pushed group document must win over the stale read")

	ident := s3a.iam.lookupByIdentityName("alice")
	require.NotNil(t, ident)
	req, err := http.NewRequest(http.MethodGet, "http://s3/bucket/obj", nil)
	require.NoError(t, err)
	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.VerifyActionPermission(req, ident, s3_constants.ACTION_READ, "bucket", "obj"),
		"a stale group push must not restore a grant the newer policy revoked")
}
