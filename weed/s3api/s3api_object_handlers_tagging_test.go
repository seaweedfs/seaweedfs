package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestPutObjectTaggingRejectsDuplicateKeys(t *testing.T) {
	s3a := newHeadBucketTestServer(t, &fakeTxnFiler{})
	body := "<Tagging><TagSet>" +
		"<Tag><Key>a</Key><Value>1</Value></Tag>" +
		"<Tag><Key>a</Key><Value>2</Value></Tag>" +
		"</TagSet></Tagging>"
	req := httptest.NewRequest(http.MethodPut, "/b/o?tagging", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": "b", "object": "o"})
	rr := httptest.NewRecorder()
	s3a.PutObjectTaggingHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "<Code>InvalidTag</Code>") {
		t.Fatalf("body = %s, want InvalidTag", rr.Body.String())
	}
}
