package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// The summary lists every updated key and passes through what the write found
// for the removals.
func TestNamespacePropertiesSummary(t *testing.T) {
	summary := namespacePropertiesSummary(nil, []string{"abc"}, map[string]string{"prop": "no"})
	if !reflect.DeepEqual(summary.Removed, []string{}) {
		t.Errorf("Removed = %v, want []", summary.Removed)
	}
	if !reflect.DeepEqual(summary.Updated, []string{"prop"}) {
		t.Errorf("Updated = %v, want [prop]", summary.Updated)
	}
	if !reflect.DeepEqual(summary.Missing, []string{"abc"}) {
		t.Errorf("Missing = %v, want [abc]", summary.Missing)
	}
}

// A key in both removals and updates is rejected before any backend call, so
// the conflict path is reachable without a filer.
func TestHandleUpdateNamespacePropertiesConflict(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/namespaces/ns/properties",
		strings.NewReader(`{"removals":["x"],"updates":{"x":"1"}}`))
	req = mux.SetURLVars(req, map[string]string{"namespace": "ns"})
	rec := httptest.NewRecorder()

	(&Server{}).handleUpdateNamespaceProperties(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Type != "UnprocessableEntityException" {
		t.Fatalf("error type = %q, want UnprocessableEntityException", resp.Error.Type)
	}
}

// postNamespaceProperties sends an Iceberg REST UpdateNamespaceProperties
// request, the call Spark's ALTER NAMESPACE .. SET/UNSET PROPERTIES makes.
func (e *conflictEnv) postNamespaceProperties(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/"+conflictBucket+"/namespaces/"+conflictNamespace+"/properties", strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	return rec
}

func (e *conflictEnv) namespaceProperties() map[string]string {
	e.t.Helper()
	var resp s3tables.GetNamespaceResponse
	e.adminDo("GetNamespace", &s3tables.GetNamespaceRequest{TableBucketARN: e.arn, Namespace: []string{conflictNamespace}}, &resp)
	return resp.Properties
}

func (e *conflictEnv) mustSetNamespaceProperties(body string) {
	e.t.Helper()
	if rec := e.postNamespaceProperties(body); rec.Code != http.StatusOK {
		e.t.Fatalf("seed %s: status %d: %s", body, rec.Code, rec.Body.String())
	}
}

// Two clients update different keys at the same time. The second lands while
// the first is between reading the properties and writing them back. Both are
// acknowledged, so both keys must be there afterwards.
func TestUpdateNamespacePropertiesKeepsConcurrentUpdate(t *testing.T) {
	e := newConflictEnv(t)
	e.mustSetNamespaceProperties(`{"updates":{"owner":"etl"}}`)

	var competing *httptest.ResponseRecorder
	e.filer.BeforeUpdate = func() {
		competing = e.postNamespaceProperties(`{"updates":{"b":"2"}}`)
	}
	rec := e.postNamespaceProperties(`{"updates":{"a":"1"}}`)

	if competing == nil {
		t.Fatal("the competing update never ran: the handler wrote nothing")
	}
	if competing.Code != http.StatusOK || rec.Code != http.StatusOK {
		t.Fatalf("statuses: competing %d %s, first %d %s", competing.Code, competing.Body.String(), rec.Code, rec.Body.String())
	}
	got := e.namespaceProperties()
	for key, want := range map[string]string{"owner": "etl", "a": "1", "b": "2"} {
		if got[key] != want {
			t.Errorf("property %s = %q, want %q (an acknowledged update was lost); properties: %v", key, got[key], want, got)
		}
	}
}

// A removal acknowledged while another client's update is in flight must stay
// removed: the update must not write the removed key back.
func TestUpdateNamespacePropertiesKeepsConcurrentRemoval(t *testing.T) {
	e := newConflictEnv(t)
	e.mustSetNamespaceProperties(`{"updates":{"owner":"etl","pii":"true"}}`)

	var competing *httptest.ResponseRecorder
	e.filer.BeforeUpdate = func() {
		competing = e.postNamespaceProperties(`{"removals":["pii"]}`)
	}
	rec := e.postNamespaceProperties(`{"updates":{"a":"1"}}`)

	if competing == nil {
		t.Fatal("the competing removal never ran: the handler wrote nothing")
	}
	if competing.Code != http.StatusOK || rec.Code != http.StatusOK {
		t.Fatalf("statuses: competing %d %s, first %d %s", competing.Code, competing.Body.String(), rec.Code, rec.Body.String())
	}
	var removed UpdateNamespacePropertiesResponse
	if err := json.Unmarshal(competing.Body.Bytes(), &removed); err != nil || len(removed.Removed) != 1 {
		t.Fatalf("competing removal summary = %s (%v), want pii removed", competing.Body.String(), err)
	}
	got := e.namespaceProperties()
	if v, ok := got["pii"]; ok {
		t.Errorf("property pii = %q came back after its acknowledged removal; properties: %v", v, got)
	}
	if got["a"] != "1" || got["owner"] != "etl" {
		t.Errorf("properties = %v, want owner=etl and a=1", got)
	}
}

// When another client's update lands before every attempt of the write, the
// update gives up having written nothing. It must answer 503 with Retry-After,
// which Iceberg clients retry on their own, not a 500 they surface as a
// failure; the resent request then lands.
func TestUpdateNamespacePropertiesAsksForRetryWhenEveryAttemptLoses(t *testing.T) {
	e := newConflictEnv(t)
	e.mustSetNamespaceProperties(`{"updates":{"owner":"etl"}}`)

	var competing []int
	inCompeting := false
	e.filer.OnUpdate = func(string, string) {
		if inCompeting {
			return
		}
		inCompeting = true
		rec := e.postNamespaceProperties(fmt.Sprintf(`{"updates":{"c%d":"x"}}`, len(competing)+1))
		inCompeting = false
		competing = append(competing, rec.Code)
	}
	rec := e.postNamespaceProperties(`{"updates":{"a":"1"}}`)
	e.filer.OnUpdate = nil

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d %s, want 503", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After header: a client may only resend the POST when it is present")
	}
	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Error.Type != "ServiceUnavailableException" {
		t.Errorf("error = %s (%v), want ServiceUnavailableException", rec.Body.String(), err)
	}
	got := e.namespaceProperties()
	if v, ok := got["a"]; ok {
		t.Errorf("property a = %q was written by an update answered 503", v)
	}
	for i, code := range competing {
		if key := fmt.Sprintf("c%d", i+1); code != http.StatusOK || got[key] != "x" {
			t.Errorf("competing update of %s: status %d, value %q; properties: %v", key, code, got[key], got)
		}
	}

	if rec := e.postNamespaceProperties(`{"updates":{"a":"1"}}`); rec.Code != http.StatusOK {
		t.Fatalf("resent update: status %d: %s", rec.Code, rec.Body.String())
	}
	if got := e.namespaceProperties(); got["a"] != "1" {
		t.Errorf("properties after the resent update = %v, want a=1", got)
	}
}

// Updates made one after another answer and store what they did before the
// changes moved into the write: the summary reports each key, keys the request
// does not name are untouched, an empty request changes nothing, and the
// namespace keeps its owner and creation time.
func TestUpdateNamespacePropertiesOneAfterAnother(t *testing.T) {
	e := newConflictEnv(t)
	var start s3tables.GetNamespaceResponse
	e.adminDo("GetNamespace", &s3tables.GetNamespaceRequest{TableBucketARN: e.arn, Namespace: []string{conflictNamespace}}, &start)

	update := func(body string, removed, updated, missing []string, stored map[string]string) {
		t.Helper()
		rec := e.postNamespaceProperties(body)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", body, rec.Code, rec.Body.String())
		}
		for _, field := range []string{`"removed":`, `"updated":`, `"missing":`} {
			if !strings.Contains(rec.Body.String(), field) || strings.Contains(rec.Body.String(), field+"null") {
				t.Errorf("%s: summary %s must carry %s as a list", body, rec.Body.String(), field)
			}
		}
		var got UpdateNamespacePropertiesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("%s: decode: %v", body, err)
		}
		sort.Strings(got.Updated)
		want := UpdateNamespacePropertiesResponse{Removed: removed, Updated: updated, Missing: missing}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: summary = %+v, want %+v", body, got, want)
		}
		if props := e.namespaceProperties(); !(len(props) == 0 && len(stored) == 0) && !reflect.DeepEqual(props, stored) {
			t.Errorf("%s: stored properties = %v, want %v", body, props, stored)
		}
	}

	update(`{"updates":{"foo":"bar","prop":"yes"}}`, []string{}, []string{"foo", "prop"}, []string{},
		map[string]string{"foo": "bar", "prop": "yes"})
	update(`{"removals":["foo","nope","foo"],"updates":{"prop":"no","new":"1"}}`, []string{"foo"}, []string{"new", "prop"}, []string{"nope"},
		map[string]string{"prop": "no", "new": "1"})
	update(`{}`, []string{}, []string{}, []string{}, map[string]string{"prop": "no", "new": "1"})
	update(`{"removals":["prop","new"]}`, []string{"prop", "new"}, []string{}, []string{}, map[string]string{})

	var end s3tables.GetNamespaceResponse
	e.adminDo("GetNamespace", &s3tables.GetNamespaceRequest{TableBucketARN: e.arn, Namespace: []string{conflictNamespace}}, &end)
	if !end.CreatedAt.Equal(start.CreatedAt) || end.OwnerAccountID != start.OwnerAccountID {
		t.Errorf("namespace became created %v by %q, was %v by %q", end.CreatedAt, end.OwnerAccountID, start.CreatedAt, start.OwnerAccountID)
	}
}

// A missing namespace answers 404 and a body that is not JSON 400, as before.
func TestUpdateNamespacePropertiesErrors(t *testing.T) {
	e := newConflictEnv(t)
	for _, tc := range []struct {
		namespace, body, errType string
		status                   int
	}{
		{"nosuch", `{"updates":{"a":"1"}}`, "NoSuchNamespaceException", http.StatusNotFound},
		{conflictNamespace, `{"updates":`, "BadRequestException", http.StatusBadRequest},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/"+conflictBucket+"/namespaces/"+tc.namespace+"/properties", strings.NewReader(tc.body))
		rec := httptest.NewRecorder()
		e.router.ServeHTTP(rec, req)
		var resp ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || rec.Code != tc.status || resp.Error.Type != tc.errType {
			t.Errorf("%s %s: %d %s, want %d %s", tc.namespace, tc.body, rec.Code, rec.Body.String(), tc.status, tc.errType)
		}
	}
	if props := e.namespaceProperties(); len(props) != 0 {
		t.Errorf("a refused update wrote properties %v", props)
	}
}
