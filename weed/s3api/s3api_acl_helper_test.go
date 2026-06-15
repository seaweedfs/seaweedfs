package s3api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// stubAccountManager satisfies AccountManager for ACL helper tests: any
// non-empty id resolves to a name so canonical-user grants validate.
type stubAccountManager struct{}

func (stubAccountManager) GetAccountNameById(id string) string {
	if id == "" {
		return ""
	}
	return "account-" + id
}

func (stubAccountManager) GetAccountIdByEmail(string) string { return "" }

// A canned-ACL request carries the ACL in the x-amz-acl header and no ACP body.
// Newer AWS SDKs (default request checksums) still attach a body — an empty
// payload or one wrapped in aws-chunked framing with a checksum trailer. Since
// PutObjectAcl does not de-chunk, ExtractAcl must honor the canned header
// instead of XML-parsing that body and returning InvalidRequest.
func TestExtractAclCannedAclWithNonEmptyBody(t *testing.T) {
	const accountId = "testAccount"
	// Not valid AccessControlPolicy XML: stands in for aws-chunked framing /
	// checksum trailer that the SDK attaches to a header-only request.
	framedBody := "0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"

	r, err := http.NewRequest(http.MethodPut, "http://example.com/bucket/object?acl", strings.NewReader(framedBody))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(s3_constants.AmzCannedAcl, s3_constants.CannedAclPrivate)

	grants, errCode := ExtractAcl(r, stubAccountManager{}, "", accountId, accountId, accountId)
	if errCode != s3err.ErrNone {
		t.Fatalf("canned ACL with non-empty body: expected ErrNone, got %v", errCode)
	}
	if len(grants) != 1 {
		t.Fatalf("canned private should yield 1 grant (owner full control), got %d", len(grants))
	}
}

// Without a canned ACL header, ExtractAcl must still treat the body as an
// AccessControlPolicy — a non-XML body is rejected with InvalidRequest. This
// guards that the canned-header fast path didn't bypass body parsing entirely.
func TestExtractAclNonXmlBodyWithoutCannedHeader(t *testing.T) {
	r, err := http.NewRequest(http.MethodPut, "http://example.com/bucket/object?acl", strings.NewReader("not-acp-xml"))
	if err != nil {
		t.Fatal(err)
	}

	_, errCode := ExtractAcl(r, stubAccountManager{}, "", "owner", "owner", "owner")
	if errCode != s3err.ErrInvalidRequest {
		t.Fatalf("non-XML body without canned header: expected ErrInvalidRequest, got %v", errCode)
	}
}
