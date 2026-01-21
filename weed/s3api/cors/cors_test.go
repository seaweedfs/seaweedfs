package cors

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestValidateConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		config  *CORSConfiguration
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "empty rules",
			config: &CORSConfiguration{
				CORSRules: []CORSRule{},
			},
			wantErr: true,
		},
		{
			name: "valid single rule",
			config: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedMethods: []string{"GET", "POST"},
						AllowedOrigins: []string{"*"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "too many rules",
			config: &CORSConfiguration{
				CORSRules: make([]CORSRule, 101),
			},
			wantErr: true,
		},
		{
			name: "invalid method",
			config: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedMethods: []string{"INVALID"},
						AllowedOrigins: []string{"*"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty origins",
			config: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedMethods: []string{"GET"},
						AllowedOrigins: []string{},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid origin with multiple wildcards",
			config: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedMethods: []string{"GET"},
						AllowedOrigins: []string{"http://*.*.example.com"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "negative MaxAgeSeconds",
			config: &CORSConfiguration{
				CORSRules: []CORSRule{
					{
						AllowedMethods: []string{"GET"},
						AllowedOrigins: []string{"*"},
						MaxAgeSeconds:  intPtr(-1),
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfiguration(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfiguration() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateOrigin(t *testing.T) {
	tests := []struct {
		name    string
		origin  string
		wantErr bool
	}{
		{
			name:    "empty origin",
			origin:  "",
			wantErr: true,
		},
		{
			name:    "valid origin",
			origin:  "http://example.com",
			wantErr: false,
		},
		{
			name:    "wildcard origin",
			origin:  "*",
			wantErr: false,
		},
		{
			name:    "valid wildcard origin",
			origin:  "http://*.example.com",
			wantErr: false,
		},
		{
			name:    "https wildcard origin",
			origin:  "https://*.example.com",
			wantErr: false,
		},
		{
			name:    "invalid wildcard origin",
			origin:  "*.example.com",
			wantErr: true,
		},
		{
			name:    "multiple wildcards",
			origin:  "http://*.*.example.com",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOrigin(tt.origin)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOrigin() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want *CORSRequest
	}{
		{
			name: "simple GET request",
			req: &http.Request{
				Method: "GET",
				Header: http.Header{
					"Origin": []string{"http://example.com"},
				},
			},
			want: &CORSRequest{
				Origin:             "http://example.com",
				Method:             "GET",
				IsPreflightRequest: false,
			},
		},
		{
			name: "OPTIONS preflight request",
			req: &http.Request{
				Method: "OPTIONS",
				Header: http.Header{
					"Origin":                         []string{"http://example.com"},
					"Access-Control-Request-Method":  []string{"PUT"},
					"Access-Control-Request-Headers": []string{"Content-Type, Authorization"},
				},
			},
			want: &CORSRequest{
				Origin:                      "http://example.com",
				Method:                      "OPTIONS",
				IsPreflightRequest:          true,
				AccessControlRequestMethod:  "PUT",
				AccessControlRequestHeaders: []string{"Content-Type", "Authorization"},
			},
		},
		{
			name: "request without origin",
			req: &http.Request{
				Method: "GET",
				Header: http.Header{},
			},
			want: &CORSRequest{
				Origin:             "",
				Method:             "GET",
				IsPreflightRequest: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseRequest(tt.req)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesOrigin(t *testing.T) {
	tests := []struct {
		name           string
		allowedOrigins []string
		origin         string
		want           bool
	}{
		{
			name:           "wildcard match",
			allowedOrigins: []string{"*"},
			origin:         "http://example.com",
			want:           true,
		},
		{
			name:           "exact match",
			allowedOrigins: []string{"http://example.com"},
			origin:         "http://example.com",
			want:           true,
		},
		{
			name:           "no match",
			allowedOrigins: []string{"http://example.com"},
			origin:         "http://other.com",
			want:           false,
		},
		{
			name:           "wildcard subdomain match",
			allowedOrigins: []string{"http://*.example.com"},
			origin:         "http://api.example.com",
			want:           true,
		},
		{
			name:           "wildcard subdomain no match",
			allowedOrigins: []string{"http://*.example.com"},
			origin:         "http://example.com",
			want:           false,
		},
		{
			name:           "multiple origins with match",
			allowedOrigins: []string{"http://example.com", "http://other.com"},
			origin:         "http://other.com",
			want:           true,
		},
		// HTTPS test cases
		{
			name:           "https exact match",
			allowedOrigins: []string{"https://example.com"},
			origin:         "https://example.com",
			want:           true,
		},
		{
			name:           "https no match",
			allowedOrigins: []string{"https://example.com"},
			origin:         "https://other.com",
			want:           false,
		},
		{
			name:           "https wildcard subdomain match",
			allowedOrigins: []string{"https://*.example.com"},
			origin:         "https://api.example.com",
			want:           true,
		},
		{
			name:           "https wildcard subdomain no match - base domain",
			allowedOrigins: []string{"https://*.example.com"},
			origin:         "https://example.com",
			want:           false,
		},
		{
			name:           "https wildcard subdomain no match - different domain",
			allowedOrigins: []string{"https://*.example.com"},
			origin:         "https://api.other.com",
			want:           false,
		},
		{
			name:           "protocol mismatch - http pattern https origin",
			allowedOrigins: []string{"http://*.example.com"},
			origin:         "https://api.example.com",
			want:           false,
		},
		{
			name:           "protocol mismatch - https pattern http origin",
			allowedOrigins: []string{"https://*.example.com"},
			origin:         "http://api.example.com",
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesOrigin(tt.allowedOrigins, tt.origin)
			if got != tt.want {
				t.Errorf("matchesOrigin() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesHeader(t *testing.T) {
	tests := []struct {
		name           string
		allowedHeaders []string
		header         string
		want           bool
	}{
		{
			name:           "empty allowed headers",
			allowedHeaders: []string{},
			header:         "Content-Type",
			want:           true,
		},
		{
			name:           "wildcard match",
			allowedHeaders: []string{"*"},
			header:         "Content-Type",
			want:           true,
		},
		{
			name:           "exact match",
			allowedHeaders: []string{"Content-Type"},
			header:         "Content-Type",
			want:           true,
		},
		{
			name:           "case insensitive match",
			allowedHeaders: []string{"content-type"},
			header:         "Content-Type",
			want:           true,
		},
		{
			name:           "no match",
			allowedHeaders: []string{"Authorization"},
			header:         "Content-Type",
			want:           false,
		},
		{
			name:           "wildcard prefix match",
			allowedHeaders: []string{"x-amz-*"},
			header:         "x-amz-date",
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesHeader(tt.allowedHeaders, tt.header)
			if got != tt.want {
				t.Errorf("matchesHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluateRequest(t *testing.T) {
	config := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedMethods: []string{"GET", "POST"},
				AllowedOrigins: []string{"http://example.com"},
				AllowedHeaders: []string{"Content-Type"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  intPtr(3600),
			},
			{
				AllowedMethods: []string{"PUT"},
				AllowedOrigins: []string{"*"},
			},
		},
	}

	tests := []struct {
		name    string
		config  *CORSConfiguration
		corsReq *CORSRequest
		want    *CORSResponse
		wantErr bool
	}{
		{
			name:   "matching first rule",
			config: config,
			corsReq: &CORSRequest{
				Origin: "http://example.com",
				Method: "GET",
			},
			want: &CORSResponse{
				AllowOrigin:   "http://example.com",
				AllowMethods:  "GET, POST",
				AllowHeaders:  "Content-Type",
				ExposeHeaders: "ETag",
				MaxAge:        "3600",
			},
			wantErr: false,
		},
		{
			name:   "matching second rule",
			config: config,
			corsReq: &CORSRequest{
				Origin: "http://other.com",
				Method: "PUT",
			},
			want: &CORSResponse{
				AllowOrigin:  "http://other.com",
				AllowMethods: "PUT",
			},
			wantErr: false,
		},
		{
			name:   "no matching rule",
			config: config,
			corsReq: &CORSRequest{
				Origin: "http://forbidden.com",
				Method: "GET",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "preflight request",
			config: config,
			corsReq: &CORSRequest{
				Origin:                      "http://example.com",
				Method:                      "OPTIONS",
				IsPreflightRequest:          true,
				AccessControlRequestMethod:  "POST",
				AccessControlRequestHeaders: []string{"Content-Type"},
			},
			want: &CORSResponse{
				AllowOrigin:   "http://example.com",
				AllowMethods:  "GET, POST",
				AllowHeaders:  "Content-Type",
				ExposeHeaders: "ETag",
				MaxAge:        "3600",
			},
			wantErr: false,
		},
		{
			name:   "preflight request with forbidden header",
			config: config,
			corsReq: &CORSRequest{
				Origin:                      "http://example.com",
				Method:                      "OPTIONS",
				IsPreflightRequest:          true,
				AccessControlRequestMethod:  "POST",
				AccessControlRequestHeaders: []string{"Authorization"},
			},
			want: &CORSResponse{
				AllowOrigin: "http://example.com",
				// No AllowMethods or AllowHeaders because the requested header is forbidden
			},
			wantErr: false,
		},
		{
			name:   "request without origin",
			config: config,
			corsReq: &CORSRequest{
				Origin: "",
				Method: "GET",
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvaluateRequest(tt.config, tt.corsReq)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvaluateRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvaluateRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyHeaders(t *testing.T) {
	tests := []struct {
		name     string
		corsResp *CORSResponse
		want     map[string]string
	}{
		{
			name:     "nil response",
			corsResp: nil,
			want:     map[string]string{},
		},
		{
			name: "complete response",
			corsResp: &CORSResponse{
				AllowOrigin:   "http://example.com",
				AllowMethods:  "GET, POST",
				AllowHeaders:  "Content-Type",
				ExposeHeaders: "ETag",
				MaxAge:        "3600",
			},
			want: map[string]string{
				"Access-Control-Allow-Origin":   "http://example.com",
				"Access-Control-Allow-Methods":  "GET, POST",
				"Access-Control-Allow-Headers":  "Content-Type",
				"Access-Control-Expose-Headers": "ETag",
				"Access-Control-Max-Age":        "3600",
			},
		},
		{
			name: "with credentials",
			corsResp: &CORSResponse{
				AllowOrigin:      "http://example.com",
				AllowMethods:     "GET",
				AllowCredentials: true,
			},
			want: map[string]string{
				"Access-Control-Allow-Origin":      "http://example.com",
				"Access-Control-Allow-Methods":     "GET",
				"Access-Control-Allow-Credentials": "true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a proper response writer using httptest
			w := httptest.NewRecorder()

			ApplyHeaders(w, tt.corsResp)

			// Extract headers from the response
			headers := make(map[string]string)
			for key, values := range w.Header() {
				if len(values) > 0 {
					headers[key] = values[0]
				}
			}

			if !reflect.DeepEqual(headers, tt.want) {
				t.Errorf("ApplyHeaders() headers = %v, want %v", headers, tt.want)
			}
		})
	}
}

// Helper functions and types for testing

func intPtr(i int) *int {
	return &i
}
