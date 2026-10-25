package iceberg

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
)

// The admin console tells users that every table bucket is its own Iceberg
// catalog, so the /v1/config handshake must turn whichever spelling of that
// bucket the client sends into overrides.prefix. PyIceberg only sends the
// warehouse on this one call, so a dropped value leaves it talking to the
// default bucket for the rest of the session.
func TestHandleConfigWarehouseSpellings(t *testing.T) {
	tests := []struct {
		name          string
		warehouse     string
		wantPrefix    string
		wantWarehouse string
	}{
		{
			name:          "s3 location",
			warehouse:     "s3://seaweed-iceberg/",
			wantPrefix:    "seaweed-iceberg",
			wantWarehouse: "s3://seaweed-iceberg",
		},
		{
			name:          "bare bucket name",
			warehouse:     "seaweed-iceberg",
			wantPrefix:    "seaweed-iceberg",
			wantWarehouse: "s3://seaweed-iceberg",
		},
		{
			name:          "table bucket ARN",
			warehouse:     "arn:aws:s3tables:us-east-1:admin:bucket/seaweed-iceberg",
			wantPrefix:    "seaweed-iceberg",
			wantWarehouse: "s3://seaweed-iceberg",
		},
		{
			name:      "no warehouse",
			warehouse: "",
		},
		{
			name:      "unusable value",
			warehouse: "file:///tmp/wh",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := "/v1/config"
			if tc.warehouse != "" {
				target += "?" + url.Values{"warehouse": {tc.warehouse}}.Encode()
			}
			r := httptest.NewRequest("GET", target, nil)
			rec := httptest.NewRecorder()
			(&Server{}).handleConfig(rec, r)

			var config CatalogConfig
			if err := json.Unmarshal(rec.Body.Bytes(), &config); err != nil {
				t.Fatalf("decode config: %v", err)
			}
			if got := config.Overrides["prefix"]; got != tc.wantPrefix {
				t.Errorf("overrides.prefix = %q, want %q", got, tc.wantPrefix)
			}
			if got := config.Defaults["warehouse"]; got != tc.wantWarehouse {
				t.Errorf("defaults.warehouse = %q, want %q", got, tc.wantWarehouse)
			}
		})
	}
}
