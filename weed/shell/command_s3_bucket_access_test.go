package shell

import (
	"bytes"
	"slices"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

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
