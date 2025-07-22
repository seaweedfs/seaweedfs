package cors

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// CORSRule represents a single CORS rule
type CORSRule struct {
	AllowedHeaders []string `xml:"AllowedHeader,omitempty" json:"AllowedHeaders,omitempty"`
	AllowedMethods []string `xml:"AllowedMethod" json:"AllowedMethods"`
	AllowedOrigins []string `xml:"AllowedOrigin" json:"AllowedOrigins"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty" json:"ExposeHeaders,omitempty"`
	MaxAgeSeconds  *int     `xml:"MaxAgeSeconds,omitempty" json:"MaxAgeSeconds,omitempty"`
	ID             string   `xml:"ID,omitempty" json:"ID,omitempty"`
}

// CORSConfiguration represents the CORS configuration for a bucket
type CORSConfiguration struct {
	CORSRules []CORSRule `xml:"CORSRule" json:"CORSRules"`
}

// CORSRequest represents a CORS request
type CORSRequest struct {
	Origin                      string
	Method                      string
	RequestHeaders              []string
	IsPreflightRequest          bool
	AccessControlRequestMethod  string
	AccessControlRequestHeaders []string
}

// CORSResponse represents the response for a CORS request
type CORSResponse struct {
	AllowOrigin      string
	AllowMethods     string
	AllowHeaders     string
	ExposeHeaders    string
	MaxAge           string
	AllowCredentials bool
}

// ValidateConfiguration validates a CORS configuration
func ValidateConfiguration(config *CORSConfiguration) error {
	if config == nil {
		return fmt.Errorf("CORS configuration cannot be nil")
	}

	if len(config.CORSRules) == 0 {
		return fmt.Errorf("CORS configuration must have at least one rule")
	}

	if len(config.CORSRules) > 100 {
		return fmt.Errorf("CORS configuration cannot have more than 100 rules")
	}

	for i, rule := range config.CORSRules {
		if err := validateRule(&rule); err != nil {
			return fmt.Errorf("invalid CORS rule at index %d: %v", i, err)
		}
	}

	return nil
}

// ParseRequest parses an HTTP request to extract CORS information
func ParseRequest(r *http.Request) *CORSRequest {
	corsReq := &CORSRequest{
		Origin: r.Header.Get("Origin"),
		Method: r.Method,
	}

	// Check if this is a preflight request
	if r.Method == "OPTIONS" {
		corsReq.IsPreflightRequest = true
		corsReq.AccessControlRequestMethod = r.Header.Get("Access-Control-Request-Method")

		if headers := r.Header.Get("Access-Control-Request-Headers"); headers != "" {
			corsReq.AccessControlRequestHeaders = strings.Split(headers, ",")
			for i := range corsReq.AccessControlRequestHeaders {
				corsReq.AccessControlRequestHeaders[i] = strings.TrimSpace(corsReq.AccessControlRequestHeaders[i])
			}
		}
	}

	return corsReq
}

// validateRule validates a single CORS rule
func validateRule(rule *CORSRule) error {
	if len(rule.AllowedMethods) == 0 {
		return fmt.Errorf("AllowedMethods cannot be empty")
	}

	if len(rule.AllowedOrigins) == 0 {
		return fmt.Errorf("AllowedOrigins cannot be empty")
	}

	// Validate allowed methods
	validMethods := map[string]bool{
		"GET":    true,
		"PUT":    true,
		"POST":   true,
		"DELETE": true,
		"HEAD":   true,
	}

	for _, method := range rule.AllowedMethods {
		if !validMethods[method] {
			return fmt.Errorf("invalid HTTP method: %s", method)
		}
	}

	// Validate origins
	for _, origin := range rule.AllowedOrigins {
		if origin == "*" {
			continue
		}
		if err := validateOrigin(origin); err != nil {
			return fmt.Errorf("invalid origin %s: %v", origin, err)
		}
	}

	// Validate MaxAgeSeconds
	if rule.MaxAgeSeconds != nil && *rule.MaxAgeSeconds < 0 {
		return fmt.Errorf("MaxAgeSeconds cannot be negative")
	}

	return nil
}

// validateOrigin validates an origin string
func validateOrigin(origin string) error {
	if origin == "" {
		return fmt.Errorf("origin cannot be empty")
	}

	// Special case: "*" is always valid
	if origin == "*" {
		return nil
	}

	// Count wildcards
	wildcardCount := strings.Count(origin, "*")
	if wildcardCount > 1 {
		return fmt.Errorf("origin can contain at most one wildcard")
	}

	// If there's a wildcard, it should be in a valid position
	if wildcardCount == 1 {
		// Must be in the format: http://*.example.com or https://*.example.com
		if !strings.HasPrefix(origin, "http://") && !strings.HasPrefix(origin, "https://") {
			return fmt.Errorf("origin with wildcard must start with http:// or https://")
		}
	}

	return nil
}

// EvaluateRequest evaluates a CORS request against a CORS configuration
func EvaluateRequest(config *CORSConfiguration, corsReq *CORSRequest) (*CORSResponse, error) {
	if config == nil || corsReq == nil {
		return nil, fmt.Errorf("config and corsReq cannot be nil")
	}

	if corsReq.Origin == "" {
		return nil, fmt.Errorf("origin header is required for CORS requests")
	}

	// Find the first rule that matches the origin
	for _, rule := range config.CORSRules {
		if matchesOrigin(rule.AllowedOrigins, corsReq.Origin) {
			// For preflight requests, we need more detailed validation
			if corsReq.IsPreflightRequest {
				return buildPreflightResponse(&rule, corsReq), nil
			} else {
				// For actual requests, check method
				if containsString(rule.AllowedMethods, corsReq.Method) {
					return buildResponse(&rule, corsReq), nil
				}
			}
		}
	}

	return nil, fmt.Errorf("no matching CORS rule found")
}

// buildPreflightResponse builds a CORS response for preflight requests
func buildPreflightResponse(rule *CORSRule, corsReq *CORSRequest) *CORSResponse {
	response := &CORSResponse{
		AllowOrigin: corsReq.Origin,
	}

	// Check if the requested method is allowed
	methodAllowed := corsReq.AccessControlRequestMethod == "" || containsString(rule.AllowedMethods, corsReq.AccessControlRequestMethod)

	// Check requested headers
	var allowedRequestHeaders []string
	allHeadersAllowed := true

	if len(corsReq.AccessControlRequestHeaders) > 0 {
		// Check if wildcard is allowed
		hasWildcard := false
		for _, header := range rule.AllowedHeaders {
			if header == "*" {
				hasWildcard = true
				break
			}
		}

		if hasWildcard {
			// All requested headers are allowed with wildcard
			allowedRequestHeaders = corsReq.AccessControlRequestHeaders
		} else {
			// Check each requested header individually
			for _, requestedHeader := range corsReq.AccessControlRequestHeaders {
				if matchesHeader(rule.AllowedHeaders, requestedHeader) {
					allowedRequestHeaders = append(allowedRequestHeaders, requestedHeader)
				} else {
					allHeadersAllowed = false
				}
			}
		}
	}

	// Only set method and header info if both method and ALL headers are allowed
	if methodAllowed && allHeadersAllowed {
		response.AllowMethods = strings.Join(rule.AllowedMethods, ", ")

		if len(allowedRequestHeaders) > 0 {
			response.AllowHeaders = strings.Join(allowedRequestHeaders, ", ")
		}

		// Set exposed headers
		if len(rule.ExposeHeaders) > 0 {
			response.ExposeHeaders = strings.Join(rule.ExposeHeaders, ", ")
		}

		// Set max age
		if rule.MaxAgeSeconds != nil {
			response.MaxAge = strconv.Itoa(*rule.MaxAgeSeconds)
		}
	}

	return response
}

// buildResponse builds a CORS response from a matching rule
func buildResponse(rule *CORSRule, corsReq *CORSRequest) *CORSResponse {
	response := &CORSResponse{
		AllowOrigin: corsReq.Origin,
	}

	// Set allowed methods
	response.AllowMethods = strings.Join(rule.AllowedMethods, ", ")

	// Set allowed headers
	if len(rule.AllowedHeaders) > 0 {
		response.AllowHeaders = strings.Join(rule.AllowedHeaders, ", ")
	}

	// Set expose headers
	if len(rule.ExposeHeaders) > 0 {
		response.ExposeHeaders = strings.Join(rule.ExposeHeaders, ", ")
	}

	// Set max age
	if rule.MaxAgeSeconds != nil {
		response.MaxAge = strconv.Itoa(*rule.MaxAgeSeconds)
	}

	return response
}

// Helper functions

// matchesOrigin checks if the request origin matches any allowed origin
func matchesOrigin(allowedOrigins []string, origin string) bool {
	for _, allowedOrigin := range allowedOrigins {
		if allowedOrigin == "*" {
			return true
		}
		if allowedOrigin == origin {
			return true
		}
		// Handle wildcard patterns like https://*.example.com
		if strings.Contains(allowedOrigin, "*") {
			if matchWildcard(allowedOrigin, origin) {
				return true
			}
		}
	}
	return false
}

// matchWildcard performs wildcard matching for origins
func matchWildcard(pattern, text string) bool {
	// Simple wildcard matching - only supports single * at the beginning
	if strings.HasPrefix(pattern, "http://*") {
		suffix := pattern[8:] // Remove "http://*"
		return strings.HasPrefix(text, "http://") && strings.HasSuffix(text, suffix)
	}
	if strings.HasPrefix(pattern, "https://*") {
		suffix := pattern[9:] // Remove "https://*"
		return strings.HasPrefix(text, "https://") && strings.HasSuffix(text, suffix)
	}
	return false
}

// matchesHeader checks if a header is allowed
func matchesHeader(allowedHeaders []string, header string) bool {
	// If no headers are specified, all headers are allowed
	if len(allowedHeaders) == 0 {
		return true
	}

	// Header matching is case-insensitive
	header = strings.ToLower(header)

	for _, allowedHeader := range allowedHeaders {
		allowedHeaderLower := strings.ToLower(allowedHeader)

		// Wildcard match
		if allowedHeaderLower == "*" {
			return true
		}

		// Exact match
		if allowedHeaderLower == header {
			return true
		}

		// Prefix wildcard match (e.g., "x-amz-*" matches "x-amz-date")
		if strings.HasSuffix(allowedHeaderLower, "*") {
			prefix := strings.TrimSuffix(allowedHeaderLower, "*")
			if strings.HasPrefix(header, prefix) {
				return true
			}
		}
	}
	return false
}

// containsString checks if a slice contains a specific string
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ApplyHeaders applies CORS headers to an HTTP response
func ApplyHeaders(w http.ResponseWriter, corsResp *CORSResponse) {
	if corsResp == nil {
		return
	}

	if corsResp.AllowOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", corsResp.AllowOrigin)
	}

	if corsResp.AllowMethods != "" {
		w.Header().Set("Access-Control-Allow-Methods", corsResp.AllowMethods)
	}

	if corsResp.AllowHeaders != "" {
		w.Header().Set("Access-Control-Allow-Headers", corsResp.AllowHeaders)
	}

	if corsResp.ExposeHeaders != "" {
		w.Header().Set("Access-Control-Expose-Headers", corsResp.ExposeHeaders)
	}

	if corsResp.MaxAge != "" {
		w.Header().Set("Access-Control-Max-Age", corsResp.MaxAge)
	}

	if corsResp.AllowCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}
