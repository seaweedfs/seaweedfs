package dash

import "testing"

func TestPrincipalRoleArn(t *testing.T) {
	got := principalRoleArn("S3ReadOnlyRole")
	want := "arn:aws:iam::role/S3ReadOnlyRole"
	if got != want {
		t.Fatalf("principalRoleArn() = %q, want %q", got, want)
	}
}
