package cors

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// CORSRule represents a single CORS rule
type CORSRule struct {
	ID             string   `xml:"ID,omitempty" json:"ID,omitempty"`
	AllowedMethods []string `xml:"AllowedMethod" json:"AllowedMethods"`
	AllowedOrigins []string `xml:"AllowedOrigin" json:"AllowedOrigins"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty" json:"AllowedHeaders,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty" json:"ExposeHeaders,omitempty"`
	MaxAgeSeconds  *int     `xml:"MaxAgeSeconds,omitempty" json:"MaxAgeSeconds,omitempty"`
}

// CORSConfiguration represents the CORS configuration for a bucket
type CORSConfiguration struct {
	XMLName   xml.Name   `xml:"CORSConfiguration"`
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

// CORSResponse represents CORS response headers
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

// EvaluateRequest evaluates a CORS request against a CORS configuration
func EvaluateRequest(config *CORSConfiguration, corsReq *CORSRequest) (*CORSResponse, error) {
	if config == nil || corsReq == nil {
		return nil, fmt.Errorf("config and corsReq cannot be nil")
	}

	if corsReq.Origin == "" {
		return nil, fmt.Errorf("origin header is required for CORS requests")
	}

	// Find the first matching rule
	for _, rule := range config.CORSRules {
		if matchesRule(&rule, corsReq) {
			return buildResponse(&rule, corsReq), nil
		}
	}

	return nil, fmt.Errorf("no matching CORS rule found")
}

// matchesRule checks if a CORS request matches a CORS rule
func matchesRule(rule *CORSRule, corsReq *CORSRequest) bool {
	// Check origin - this is the primary matching criterion
	if !matchesOrigin(rule.AllowedOrigins, corsReq.Origin) {
		return false
	}

	// For preflight requests, we only check origin matching
	// The response building will handle filtering out disallowed methods and headers
	// This allows partial CORS responses to be sent
	if corsReq.IsPreflightRequest {
		return true
	}

	// For non-preflight requests, check method matching
	method := corsReq.Method
	if !contains(rule.AllowedMethods, method) {
		return false
	}

	return true
}

// matchesOrigin checks if an origin matches any of the allowed origins
func matchesOrigin(allowedOrigins []string, origin string) bool {
	for _, allowedOrigin := range allowedOrigins {
		if allowedOrigin == "*" {
			return true
		}

		if allowedOrigin == origin {
			return true
		}

		// Check wildcard matching
		if strings.Contains(allowedOrigin, "*") {
			if matchesWildcard(allowedOrigin, origin) {
				return true
			}
		}
	}
	return false
}

// matchesWildcard checks if an origin matches a wildcard pattern
func matchesWildcard(pattern, origin string) bool {
	// Convert wildcard pattern to regex
	escapedPattern := regexp.QuoteMeta(pattern)
	regexPattern := strings.Replace(escapedPattern, "\\*", ".*", -1)
	regex, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		return false
	}
	return regex.MatchString(origin)
}

// matchesHeader checks if a header matches allowed headers
func matchesHeader(allowedHeaders []string, header string) bool {
	if len(allowedHeaders) == 0 {
		return true // No restrictions
	}

	for _, allowedHeader := range allowedHeaders {
		if allowedHeader == "*" {
			return true
		}

		if strings.EqualFold(allowedHeader, header) {
			return true
		}

		// Check wildcard matching for headers
		if strings.Contains(allowedHeader, "*") {
			if matchesWildcard(strings.ToLower(allowedHeader), strings.ToLower(header)) {
				return true
			}
		}
	}

	return false
}

// buildResponse builds a CORS response from a matching rule
func buildResponse(rule *CORSRule, corsReq *CORSRequest) *CORSResponse {
	response := &CORSResponse{
		AllowOrigin: corsReq.Origin,
	}

	// Set allowed methods - for preflight requests, check if the requested method is allowed
	if corsReq.IsPreflightRequest && corsReq.AccessControlRequestMethod != "" {
		if contains(rule.AllowedMethods, corsReq.AccessControlRequestMethod) {
			response.AllowMethods = corsReq.AccessControlRequestMethod
		} else {
			// If the requested method is not allowed, return all allowed methods
			response.AllowMethods = strings.Join(rule.AllowedMethods, ", ")
		}
	} else {
		// For non-preflight requests, return all allowed methods
		response.AllowMethods = strings.Join(rule.AllowedMethods, ", ")
	}

	// Set allowed headers - for preflight requests, return the specific headers that were requested and are allowed
	if corsReq.IsPreflightRequest && len(corsReq.AccessControlRequestHeaders) > 0 {
		allowedHeaders := make([]string, 0)
		for _, requestedHeader := range corsReq.AccessControlRequestHeaders {
			if matchesHeader(rule.AllowedHeaders, requestedHeader) {
				allowedHeaders = append(allowedHeaders, requestedHeader)
			}
		}
		if len(allowedHeaders) > 0 {
			response.AllowHeaders = strings.Join(allowedHeaders, ", ")
		}
	} else if len(rule.AllowedHeaders) > 0 {
		// For non-preflight requests, return the allowed headers from the rule
		response.AllowHeaders = strings.Join(rule.AllowedHeaders, ", ")
	}

	// Set exposed headers
	if len(rule.ExposeHeaders) > 0 {
		response.ExposeHeaders = strings.Join(rule.ExposeHeaders, ", ")
	}

	// Set max age
	if rule.MaxAgeSeconds != nil {
		response.MaxAge = strconv.Itoa(*rule.MaxAgeSeconds)
	}

	return response
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
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

// FilerClient interface for dependency injection
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error
}

// EntryGetter interface for getting filer entries
type EntryGetter interface {
	GetEntry(directory, name string) (*filer_pb.Entry, error)
}

// Storage provides CORS configuration storage operations
type Storage struct {
	filerClient FilerClient
	entryGetter EntryGetter
	bucketsPath string
}

// NewStorage creates a new CORS storage instance
func NewStorage(filerClient FilerClient, entryGetter EntryGetter, bucketsPath string) *Storage {
	return &Storage{
		filerClient: filerClient,
		entryGetter: entryGetter,
		bucketsPath: bucketsPath,
	}
}

// Store stores CORS configuration in the filer
func (s *Storage) Store(bucket string, config *CORSConfiguration) error {
	// Store in bucket metadata
	bucketMetadataPath := fmt.Sprintf("%s/%s/.s3metadata", s.bucketsPath, bucket)

	// Get existing metadata
	existingEntry, err := s.entryGetter.GetEntry("", bucketMetadataPath)
	var metadata map[string]interface{}

	if err == nil && existingEntry != nil && len(existingEntry.Content) > 0 {
		if err := json.Unmarshal(existingEntry.Content, &metadata); err != nil {
			glog.V(1).Infof("Failed to unmarshal existing metadata: %v", err)
			metadata = make(map[string]interface{})
		}
	} else {
		metadata = make(map[string]interface{})
	}

	metadata["cors"] = config

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket metadata: %v", err)
	}

	// Store metadata
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: s.bucketsPath + "/" + bucket,
			Entry: &filer_pb.Entry{
				Name:        ".s3metadata",
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					FileMode: 0644,
				},
				Content: metadataBytes,
			},
		}

		_, err := client.CreateEntry(context.Background(), request)
		return err
	})
}

// Load loads CORS configuration from the filer
func (s *Storage) Load(bucket string) (*CORSConfiguration, error) {
	bucketMetadataPath := fmt.Sprintf("%s/%s/.s3metadata", s.bucketsPath, bucket)

	entry, err := s.entryGetter.GetEntry("", bucketMetadataPath)
	if err != nil || entry == nil {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	if len(entry.Content) == 0 {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(entry.Content, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}

	corsData, exists := metadata["cors"]
	if !exists {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	// Convert back to CORSConfiguration
	corsBytes, err := json.Marshal(corsData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CORS data: %v", err)
	}

	var config CORSConfiguration
	if err := json.Unmarshal(corsBytes, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CORS configuration: %v", err)
	}

	return &config, nil
}

// Delete deletes CORS configuration from the filer
func (s *Storage) Delete(bucket string) error {
	bucketMetadataPath := fmt.Sprintf("%s/%s/.s3metadata", s.bucketsPath, bucket)

	entry, err := s.entryGetter.GetEntry("", bucketMetadataPath)
	if err != nil || entry == nil {
		return nil // Already deleted or doesn't exist
	}

	var metadata map[string]interface{}
	if len(entry.Content) > 0 {
		if err := json.Unmarshal(entry.Content, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %v", err)
		}
	} else {
		return nil // No metadata to delete
	}

	// Remove CORS configuration
	delete(metadata, "cors")

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	// Update metadata
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: s.bucketsPath + "/" + bucket,
			Entry: &filer_pb.Entry{
				Name:        ".s3metadata",
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					FileMode: 0644,
				},
				Content: metadataBytes,
			},
		}

		_, err := client.CreateEntry(context.Background(), request)
		return err
	})
}
