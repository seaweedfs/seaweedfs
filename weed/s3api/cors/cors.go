package cors

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// S3 metadata file name constant to avoid typos and reduce duplication
const S3MetadataFileName = ".s3metadata"

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

	// Find the first rule that matches the origin
	for _, rule := range config.CORSRules {
		if matchesOrigin(rule.AllowedOrigins, corsReq.Origin) {
			// For preflight requests, we need more detailed validation
			if corsReq.IsPreflightRequest {
				return buildPreflightResponse(&rule, corsReq), nil
			} else {
				// For actual requests, check method
				if contains(rule.AllowedMethods, corsReq.Method) {
					return buildResponse(&rule, corsReq), nil
				}
			}
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

	// For preflight requests, we need to validate both the requested method and headers
	if corsReq.IsPreflightRequest {
		// Check if the requested method is allowed
		if corsReq.AccessControlRequestMethod != "" {
			if !contains(rule.AllowedMethods, corsReq.AccessControlRequestMethod) {
				return false
			}
		}

		// Check if all requested headers are allowed
		if len(corsReq.AccessControlRequestHeaders) > 0 {
			for _, requestedHeader := range corsReq.AccessControlRequestHeaders {
				if !matchesHeader(rule.AllowedHeaders, requestedHeader) {
					return false
				}
			}
		}

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
// Uses string manipulation instead of regex for better performance
func matchesWildcard(pattern, origin string) bool {
	// Handle simple cases first
	if pattern == "*" {
		return true
	}
	if pattern == origin {
		return true
	}

	// For CORS, we typically only deal with * wildcards (not ? wildcards)
	// Use string manipulation for * wildcards only (more efficient than regex)

	// Split pattern by wildcards
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		// No wildcards, exact match
		return pattern == origin
	}

	// Check if string starts with first part
	if len(parts[0]) > 0 && !strings.HasPrefix(origin, parts[0]) {
		return false
	}

	// Check if string ends with last part
	if len(parts[len(parts)-1]) > 0 && !strings.HasSuffix(origin, parts[len(parts)-1]) {
		return false
	}

	// Check middle parts
	searchStr := origin
	if len(parts[0]) > 0 {
		searchStr = searchStr[len(parts[0]):]
	}
	if len(parts[len(parts)-1]) > 0 {
		searchStr = searchStr[:len(searchStr)-len(parts[len(parts)-1])]
	}

	for i := 1; i < len(parts)-1; i++ {
		if len(parts[i]) > 0 {
			index := strings.Index(searchStr, parts[i])
			if index == -1 {
				return false
			}
			searchStr = searchStr[index+len(parts[i]):]
		}
	}

	return true
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

// buildPreflightResponse builds a CORS response for preflight requests
// This function allows partial matches - origin can match while methods/headers may not
func buildPreflightResponse(rule *CORSRule, corsReq *CORSRequest) *CORSResponse {
	response := &CORSResponse{
		AllowOrigin: corsReq.Origin,
	}

	// Check if the requested method is allowed
	methodAllowed := corsReq.AccessControlRequestMethod == "" || contains(rule.AllowedMethods, corsReq.AccessControlRequestMethod)

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

	// Set allowed methods - for preflight requests, return all allowed methods
	if corsReq.IsPreflightRequest {
		response.AllowMethods = strings.Join(rule.AllowedMethods, ", ")
	} else {
		// For non-preflight requests, return all allowed methods
		response.AllowMethods = strings.Join(rule.AllowedMethods, ", ")
	}

	// Set allowed headers
	if corsReq.IsPreflightRequest && len(rule.AllowedHeaders) > 0 {
		// For preflight requests, check if wildcard is allowed
		hasWildcard := false
		for _, header := range rule.AllowedHeaders {
			if header == "*" {
				hasWildcard = true
				break
			}
		}

		if hasWildcard && len(corsReq.AccessControlRequestHeaders) > 0 {
			// Return the specific headers that were requested when wildcard is allowed
			response.AllowHeaders = strings.Join(corsReq.AccessControlRequestHeaders, ", ")
		} else if len(corsReq.AccessControlRequestHeaders) > 0 {
			// For non-wildcard cases, return the requested headers (preserving case)
			// since we already validated they are allowed in matchesRule
			response.AllowHeaders = strings.Join(corsReq.AccessControlRequestHeaders, ", ")
		} else {
			// Fallback to configured headers if no specific headers were requested
			response.AllowHeaders = strings.Join(rule.AllowedHeaders, ", ")
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
	bucketMetadataPath := filepath.Join(s.bucketsPath, bucket, S3MetadataFileName)

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
		return fmt.Errorf("failed to marshal bucket metadata: %w", err)
	}

	// Store metadata
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: s.bucketsPath + "/" + bucket,
			Entry: &filer_pb.Entry{
				Name:        S3MetadataFileName,
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
	bucketMetadataPath := filepath.Join(s.bucketsPath, bucket, S3MetadataFileName)

	entry, err := s.entryGetter.GetEntry("", bucketMetadataPath)
	if err != nil || entry == nil {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	if len(entry.Content) == 0 {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(entry.Content, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	corsData, exists := metadata["cors"]
	if !exists {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	// Convert back to CORSConfiguration
	corsBytes, err := json.Marshal(corsData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CORS data: %w", err)
	}

	var config CORSConfiguration
	if err := json.Unmarshal(corsBytes, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CORS configuration: %w", err)
	}

	return &config, nil
}

// Delete deletes CORS configuration from the filer
func (s *Storage) Delete(bucket string) error {
	bucketMetadataPath := filepath.Join(s.bucketsPath, bucket, S3MetadataFileName)

	entry, err := s.entryGetter.GetEntry("", bucketMetadataPath)
	if err != nil || entry == nil {
		return nil // Already deleted or doesn't exist
	}

	var metadata map[string]interface{}
	if len(entry.Content) > 0 {
		if err := json.Unmarshal(entry.Content, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	} else {
		return nil // No metadata to delete
	}

	// Remove CORS configuration
	delete(metadata, "cors")

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Update metadata
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: s.bucketsPath + "/" + bucket,
			Entry: &filer_pb.Entry{
				Name:        S3MetadataFileName,
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
