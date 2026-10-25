package iceberg

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleRenameTableRejectsIncompleteIdentifiers(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing source name", `{"source":{"namespace":["ns"]},"destination":{"namespace":["ns"],"name":"t2"}}`},
		{"missing source namespace", `{"source":{"name":"t"},"destination":{"namespace":["ns"],"name":"t2"}}`},
		{"missing destination name", `{"source":{"namespace":["ns"],"name":"t"},"destination":{"namespace":["ns"]}}`},
		{"missing destination namespace", `{"source":{"namespace":["ns"],"name":"t"},"destination":{"name":"t2"}}`},
		{"empty body", `{}`},
		{"malformed json", `{`},
	}
	s := &Server{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/tables/rename", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			s.handleRenameTable(w, r)
			if w.Code != 400 {
				t.Fatalf("handleRenameTable() status = %d, want 400", w.Code)
			}
		})
	}
}
