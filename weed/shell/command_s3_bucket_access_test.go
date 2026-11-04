package shell

import (
	"bytes"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestCanonicalActionsAcceptAcpActions(t *testing.T) {
	names := strings.Split(supportedActionNames, ", ")
	for _, action := range []string{
		s3_constants.ACTION_READ, s3_constants.ACTION_READ_ACP,
		s3_constants.ACTION_WRITE, s3_constants.ACTION_WRITE_ACP,
		s3_constants.ACTION_LIST, s3_constants.ACTION_TAGGING, s3_constants.ACTION_ADMIN,
	} {
		for _, input := range []string{action, strings.ToLower(action), strings.ToUpper(action)} {
			if got := canonicalActions[strings.ToLower(input)]; got != action {
				t.Errorf("canonicalActions[%q] = %q, want %q", input, got, action)
			}
		}
		if !slices.Contains(names, action) {
			t.Errorf("supportedActionNames %q does not list %s", supportedActionNames, action)
		}
	}
	if len(names) != len(canonicalActions) {
		t.Errorf("supportedActionNames lists %d actions, canonicalActions has %d", len(names), len(canonicalActions))
	}
}

func TestCanonicalActionList(t *testing.T) {
	got, err := canonicalActionList(" readacp, WriteAcp,,writeacp ,list")
	if err != nil {
		t.Fatalf("canonicalActionList: %v", err)
	}
	if want := []string{"ReadAcp", "WriteAcp", "List"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if _, err := canonicalActionList("Read,PutObjectAcl"); err == nil {
		t.Error("expected an error for an unsupported action")
	}
}

func TestAnonymousSetRejectsEmptyAccess(t *testing.T) {
	err := (&commandS3AnonymousSet{}).Do([]string{"-bucket", "b", "-access", " , ,"}, nil, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "invalid -access") {
		t.Errorf("got %v, want an invalid -access error", err)
	}
}

func TestUpdateBucketActions_SetActions(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "anonymous",
		Actions: []string{},
	}

	updateBucketActions(identity, "my-bucket", "Read,List")

	expected := []string{"Read:my-bucket", "List:my-bucket"}
	sort.Strings(identity.Actions)
	sort.Strings(expected)
	if !slices.Equal(identity.Actions, expected) {
		t.Errorf("got %v, want %v", identity.Actions, expected)
	}
}

func TestUpdateBucketActions_ReplaceActions(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "anonymous",
		Actions: []string{"Read:my-bucket", "List:my-bucket"},
	}

	updateBucketActions(identity, "my-bucket", "Write")

	expected := []string{"Write:my-bucket"}
	if !slices.Equal(identity.Actions, expected) {
		t.Errorf("got %v, want %v", identity.Actions, expected)
	}
}

func TestUpdateBucketActions_AcpActions(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "testuser",
		Actions: []string{"Read:my-bucket", "Write:other-bucket"},
	}

	normalized, err := canonicalActionList("read,readacp,WRITEACP")
	if err != nil {
		t.Fatalf("canonicalActionList: %v", err)
	}
	updateBucketActions(identity, "my-bucket", strings.Join(normalized, ","))

	expected := []string{"Write:other-bucket", "Read:my-bucket", "ReadAcp:my-bucket", "WriteAcp:my-bucket"}
	if !slices.Equal(identity.Actions, expected) {
		t.Errorf("got %v, want %v", identity.Actions, expected)
	}
}

func TestUpdateBucketActions_None(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "anonymous",
		Actions: []string{"Read:my-bucket", "List:my-bucket"},
	}

	updateBucketActions(identity, "my-bucket", "none")

	if len(identity.Actions) != 0 {
		t.Errorf("expected empty actions, got %v", identity.Actions)
	}
}

func TestUpdateBucketActions_PreservesOtherBuckets(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "testuser",
		Actions: []string{"Read:bucket-a", "Write:bucket-b", "List:bucket-a"},
	}

	updateBucketActions(identity, "bucket-a", "Write")

	expected := []string{"Write:bucket-b", "Write:bucket-a"}
	sort.Strings(identity.Actions)
	sort.Strings(expected)
	if !slices.Equal(identity.Actions, expected) {
		t.Errorf("got %v, want %v", identity.Actions, expected)
	}
}

func TestUpdateBucketActions_PreservesGlobalActions(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "testuser",
		Actions: []string{"Admin", "Read:my-bucket"},
	}

	updateBucketActions(identity, "my-bucket", "none")

	expected := []string{"Admin"}
	if !slices.Equal(identity.Actions, expected) {
		t.Errorf("got %v, want %v", identity.Actions, expected)
	}
}

func TestDisplayBucketAccess(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "anonymous",
		Actions: []string{"Read:my-bucket", "List:my-bucket", "Write:other-bucket"},
	}

	var buf bytes.Buffer
	err := displayBucketAccess(&buf, "my-bucket", "anonymous", identity)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("List,Read")) {
		t.Errorf("expected 'List,Read' in output, got: %s", output)
	}
}

func TestDisplayBucketAccess_None(t *testing.T) {
	identity := &iam_pb.Identity{
		Name:    "anonymous",
		Actions: []string{"Write:other-bucket"},
	}

	var buf bytes.Buffer
	err := displayBucketAccess(&buf, "my-bucket", "anonymous", identity)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("Access: none")) {
		t.Errorf("expected 'Access: none' in output, got: %s", output)
	}
}
