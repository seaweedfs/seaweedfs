package shell

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	weediam "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func newTestS3ShellStore(t *testing.T) *memory.MemoryStore {
	t.Helper()

	store := &memory.MemoryStore{}
	if err := store.Initialize(nil, ""); err != nil {
		t.Fatalf("initialize memory store: %v", err)
	}
	return store
}

func TestRunS3UserCreateGeneratesCredentials(t *testing.T) {
	store := newTestS3ShellStore(t)

	var out bytes.Buffer
	err := runS3UserCreate(context.Background(), store, s3UserCreateOptions{
		name:                "alice",
		generateCredentials: true,
		email:               "alice@example.com",
	}, &out)
	if err != nil {
		t.Fatalf("runS3UserCreate: %v", err)
	}

	user, err := store.GetUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if len(user.Credentials) != 1 {
		t.Fatalf("expected 1 credential, got %d", len(user.Credentials))
	}
	if got := user.Credentials[0].Status; got != weediam.AccessKeyStatusActive {
		t.Fatalf("expected %q status, got %q", weediam.AccessKeyStatusActive, got)
	}
	if user.Account == nil || user.Account.EmailAddress != "alice@example.com" {
		t.Fatalf("expected account email to be set, got %+v", user.Account)
	}

	output := out.String()
	if !strings.Contains(output, `Created user "alice".`) {
		t.Fatalf("expected created message, got %q", output)
	}
	if !strings.Contains(output, "Access Key:") || !strings.Contains(output, "Secret Key:") {
		t.Fatalf("expected credential output, got %q", output)
	}
}

func TestRunS3UserSetDisabledRejectsStaticUser(t *testing.T) {
	store := newTestS3ShellStore(t)
	if err := store.CreateUser(context.Background(), &iam_pb.Identity{
		Name:     "bootstrap-admin",
		IsStatic: true,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	err := runS3UserSetDisabled(context.Background(), store, "bootstrap-admin", true, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected static user mutation to fail")
	}
	if !strings.Contains(err.Error(), "-s3.config") {
		t.Fatalf("expected static user guidance, got %v", err)
	}
}

func TestRunS3UserListIncludesSourceAndStatus(t *testing.T) {
	store := newTestS3ShellStore(t)
	if err := store.CreateUser(context.Background(), &iam_pb.Identity{
		Name: "alice",
		Credentials: []*iam_pb.Credential{
			{AccessKey: "ALICEKEY", SecretKey: "secret", Status: weediam.AccessKeyStatusActive},
		},
	}); err != nil {
		t.Fatalf("CreateUser alice: %v", err)
	}
	if err := store.CreateUser(context.Background(), &iam_pb.Identity{
		Name:     "bootstrap-admin",
		IsStatic: true,
		Disabled: true,
	}); err != nil {
		t.Fatalf("CreateUser bootstrap-admin: %v", err)
	}

	var out bytes.Buffer
	if err := runS3UserList(context.Background(), store, &out); err != nil {
		t.Fatalf("runS3UserList: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "NAME\tSOURCE\tSTATUS\tACCESS KEYS\tPOLICIES") {
		t.Fatalf("expected header, got %q", output)
	}
	if !strings.Contains(output, "alice\tdynamic\tenabled\t1\t0") {
		t.Fatalf("expected dynamic user row, got %q", output)
	}
	if !strings.Contains(output, "bootstrap-admin\tstatic\tdisabled\t0\t0") {
		t.Fatalf("expected static user row, got %q", output)
	}
}

func TestRunS3UserAccessKeyCreateGeneratesCredential(t *testing.T) {
	store := newTestS3ShellStore(t)
	if err := store.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	var out bytes.Buffer
	err := runS3UserAccessKeyCreate(context.Background(), store, s3AccessKeyCreateOptions{
		username:            "alice",
		generateCredentials: true,
	}, &out)
	if err != nil {
		t.Fatalf("runS3UserAccessKeyCreate: %v", err)
	}

	user, err := store.GetUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if len(user.Credentials) != 1 {
		t.Fatalf("expected 1 credential, got %d", len(user.Credentials))
	}
	if !strings.Contains(out.String(), `Created access key for user "alice".`) {
		t.Fatalf("expected create message, got %q", out.String())
	}
}

func TestRunS3PolicyAttachUpdatesUserPolicies(t *testing.T) {
	store := newTestS3ShellStore(t)
	if err := store.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := store.CreatePolicy(context.Background(), "photos-rw", policy_engine.PolicyDocument{
		Version: "2012-10-17",
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	var out bytes.Buffer
	if err := runS3PolicyAttach(context.Background(), store, "alice", "photos-rw", &out); err != nil {
		t.Fatalf("runS3PolicyAttach: %v", err)
	}

	user, err := store.GetUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if len(user.PolicyNames) != 1 || user.PolicyNames[0] != "photos-rw" {
		t.Fatalf("expected attached policy, got %v", user.PolicyNames)
	}
	if !strings.Contains(out.String(), `Attached policy "photos-rw" to user "alice".`) {
		t.Fatalf("expected attach message, got %q", out.String())
	}
}

func TestRunS3PolicyDetachRejectsStaticUser(t *testing.T) {
	store := newTestS3ShellStore(t)
	if err := store.CreateUser(context.Background(), &iam_pb.Identity{
		Name:        "bootstrap-admin",
		IsStatic:    true,
		PolicyNames: []string{"photos-rw"},
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := store.CreatePolicy(context.Background(), "photos-rw", policy_engine.PolicyDocument{
		Version: "2012-10-17",
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	err := runS3PolicyDetach(context.Background(), store, "bootstrap-admin", "photos-rw", &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected static user mutation to fail")
	}
	if !strings.Contains(err.Error(), "-s3.config") {
		t.Fatalf("expected static user guidance, got %v", err)
	}
}
