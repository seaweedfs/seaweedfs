package s3api

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// TestBuildRenameTableRequest covers the AWS RenameTable REST shape: the path
// names the source and the body only the destination, either half of which
// may be omitted to keep the source's.
func TestBuildRenameTableRequest(t *testing.T) {
	const encodedARN = "arn%3Aaws%3As3tables%3Aus-east-1%3A123456789012%3Abucket%2Ftest-bucket"
	cases := []struct {
		name      string
		arn       string
		namespace string
		table     string
		body      string
		want      *s3tables.RenameTableRequest
		wantErr   string
	}{
		{
			name: "newName only keeps the namespace",
			body: `{"newName":"t2"}`,
			want: &s3tables.RenameTableRequest{DestNamespace: []string{"ns"}, DestName: "t2"},
		},
		{
			name: "newNamespaceName only keeps the name",
			body: `{"newNamespaceName":"other"}`,
			want: &s3tables.RenameTableRequest{DestNamespace: []string{"other"}, DestName: "t"},
		},
		{
			name: "both destination fields",
			body: `{"newNamespaceName":"other","newName":"t2"}`,
			want: &s3tables.RenameTableRequest{DestNamespace: []string{"other"}, DestName: "t2"},
		},
		{
			name: "percent-encoded ARN is decoded",
			arn:  encodedARN,
			body: `{"newName":"t2"}`,
			want: &s3tables.RenameTableRequest{DestNamespace: []string{"ns"}, DestName: "t2"},
		},
		{
			name: "versionToken is passed through",
			body: `{"newName":"t2","versionToken":"tok-1"}`,
			want: &s3tables.RenameTableRequest{DestNamespace: []string{"ns"}, DestName: "t2", VersionToken: "tok-1"},
		},
		{
			name:    "empty body",
			body:    "",
			wantErr: "newNamespaceName or newName is required",
		},
		{
			name:    "empty object",
			body:    `{}`,
			wantErr: "newNamespaceName or newName is required",
		},
		{
			// The X-Amz-Target field names must not be read as the REST ones:
			// a body naming only destName renames nothing it asked for.
			name:    "SeaweedFS-shaped body",
			body:    `{"destNamespace":["other"],"destName":"t2"}`,
			wantErr: "newNamespaceName or newName is required",
		},
		{
			name:    "invalid newName",
			body:    `{"newName":"bad/name"}`,
			wantErr: "invalid newName",
		},
		{
			name:    "invalid newNamespaceName",
			body:    `{"newNamespaceName":".."}`,
			wantErr: "invalid newNamespaceName",
		},
		{
			name:    "empty newName beside a valid newNamespaceName",
			body:    `{"newNamespaceName":"other","newName":""}`,
			wantErr: "invalid newName",
		},
		{
			name:    "empty newNamespaceName beside a valid newName",
			body:    `{"newNamespaceName":"","newName":"t2"}`,
			wantErr: "invalid newNamespaceName",
		},
		{
			name:      "source namespace with ..",
			namespace: "..",
			body:      `{"newName":"t2"}`,
			wantErr:   "invalid path parameter namespace",
		},
		{
			name:    "invalid source name",
			table:   "..",
			body:    `{"newName":"t2"}`,
			wantErr: "invalid path parameter name",
		},
		{
			name:    "malformed body",
			body:    `{"newName":`,
			wantErr: "unexpected end of JSON input",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arn, namespace, table := testTableBucketARN, "ns", "t"
			if tc.arn != "" {
				arn = tc.arn
			}
			if tc.namespace != "" {
				namespace = tc.namespace
			}
			if tc.table != "" {
				table = tc.table
			}
			req := httptest.NewRequest(http.MethodPut, "/tables/x/ns/t/rename", strings.NewReader(tc.body))
			req = mux.SetURLVars(req, map[string]string{"tableBucketARN": arn, "namespace": namespace, "name": table})

			got, err := buildRenameTableRequest(req)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("got %+v, want an error containing %q", got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := *tc.want
			want.TableBucketARN = testTableBucketARN
			want.SourceNamespace = []string{"ns"}
			want.SourceName = "t"
			if !reflect.DeepEqual(got, &want) {
				t.Fatalf("got %+v, want %+v", got, &want)
			}
		})
	}
}
