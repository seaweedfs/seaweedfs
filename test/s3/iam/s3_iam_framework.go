package iam

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const (
	TestS3Endpoint = "http://localhost:8333"
	TestRegion     = "us-west-2"

	// Keycloak configuration
	DefaultKeycloakURL   = "http://localhost:8080"
	KeycloakRealm        = "seaweedfs-test"
	KeycloakClientID     = "seaweedfs-s3"
	KeycloakClientSecret = "seaweedfs-s3-secret"
)

// S3IAMTestFramework provides utilities for S3+IAM integration testing
type S3IAMTestFramework struct {
	t              *testing.T
	mockOIDC       *httptest.Server
	privateKey     *rsa.PrivateKey
	publicKey      *rsa.PublicKey
	createdBuckets []string
	ctx            context.Context
	keycloakClient *KeycloakClient
	useKeycloak    bool
}

// KeycloakClient handles authentication with Keycloak
type KeycloakClient struct {
	baseURL      string
	realm        string
	clientID     string
	clientSecret string
	httpClient   *http.Client
}

// KeycloakTokenResponse represents Keycloak token response
type KeycloakTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

// NewS3IAMTestFramework creates a new test framework instance
func NewS3IAMTestFramework(t *testing.T) *S3IAMTestFramework {
	framework := &S3IAMTestFramework{
		t:              t,
		ctx:            context.Background(),
		createdBuckets: make([]string, 0),
	}

	// Check if we should use Keycloak or mock OIDC
	keycloakURL := os.Getenv("KEYCLOAK_URL")
	if keycloakURL == "" {
		keycloakURL = DefaultKeycloakURL
	}

	// Test if Keycloak is available
	framework.useKeycloak = framework.isKeycloakAvailable(keycloakURL)

	if framework.useKeycloak {
		t.Logf("Using real Keycloak instance at %s", keycloakURL)
		framework.keycloakClient = NewKeycloakClient(keycloakURL, KeycloakRealm, KeycloakClientID, KeycloakClientSecret)
	} else {
		t.Logf("Using mock OIDC server for testing")
		// Generate RSA keys for JWT signing (mock mode)
		var err error
		framework.privateKey, err = rsa.GenerateKey(cryptorand.Reader, 2048)
		require.NoError(t, err)
		framework.publicKey = &framework.privateKey.PublicKey

		// Setup mock OIDC server
		framework.setupMockOIDCServer()
	}

	return framework
}

// NewKeycloakClient creates a new Keycloak client
func NewKeycloakClient(baseURL, realm, clientID, clientSecret string) *KeycloakClient {
	return &KeycloakClient{
		baseURL:      baseURL,
		realm:        realm,
		clientID:     clientID,
		clientSecret: clientSecret,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// isKeycloakAvailable checks if Keycloak is running and accessible
func (f *S3IAMTestFramework) isKeycloakAvailable(keycloakURL string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	// Use realms endpoint instead of health/ready for Keycloak v26+
	// First, verify master realm is reachable
	masterURL := fmt.Sprintf("%s/realms/master", keycloakURL)

	resp, err := client.Get(masterURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}

	// Also ensure the specific test realm exists; otherwise fall back to mock
	testRealmURL := fmt.Sprintf("%s/realms/%s", keycloakURL, KeycloakRealm)
	resp2, err := client.Get(testRealmURL)
	if err != nil {
		return false
	}
	defer resp2.Body.Close()
	return resp2.StatusCode == http.StatusOK
}

// AuthenticateUser authenticates a user with Keycloak and returns an access token
func (kc *KeycloakClient) AuthenticateUser(username, password string) (*KeycloakTokenResponse, error) {
	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", kc.baseURL, kc.realm)

	data := url.Values{}
	data.Set("grant_type", "password")
	data.Set("client_id", kc.clientID)
	data.Set("client_secret", kc.clientSecret)
	data.Set("username", username)
	data.Set("password", password)
	data.Set("scope", "openid profile email")

	resp, err := kc.httpClient.PostForm(tokenURL, data)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate with Keycloak: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Read the response body for debugging
		body, readErr := io.ReadAll(resp.Body)
		bodyStr := ""
		if readErr == nil {
			bodyStr = string(body)
		}
		return nil, fmt.Errorf("Keycloak authentication failed with status: %d, response: %s", resp.StatusCode, bodyStr)
	}

	var tokenResp KeycloakTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	return &tokenResp, nil
}

// getKeycloakToken authenticates with Keycloak and returns a JWT token
func (f *S3IAMTestFramework) getKeycloakToken(username string) (string, error) {
	if f.keycloakClient == nil {
		return "", fmt.Errorf("Keycloak client not initialized")
	}

	// Map username to password for test users
	password := f.getTestUserPassword(username)
	if password == "" {
		return "", fmt.Errorf("unknown test user: %s", username)
	}

	tokenResp, err := f.keycloakClient.AuthenticateUser(username, password)
	if err != nil {
		return "", fmt.Errorf("failed to authenticate user %s: %w", username, err)
	}

	return tokenResp.AccessToken, nil
}

// getTestUserPassword returns the password for test users
func (f *S3IAMTestFramework) getTestUserPassword(username string) string {
	// Password generation matches setup_keycloak_docker.sh logic:
	// password="${username//[^a-zA-Z]/}123" (removes non-alphabetic chars + "123")
	userPasswords := map[string]string{
		"admin-user":      "adminuser123",     // "admin-user" -> "adminuser" + "123"
		"read-user":       "readuser123",      // "read-user" -> "readuser" + "123"
		"write-user":      "writeuser123",     // "write-user" -> "writeuser" + "123"
		"write-only-user": "writeonlyuser123", // "write-only-user" -> "writeonlyuser" + "123"
	}

	return userPasswords[username]
}

// setupMockOIDCServer creates a mock OIDC server for testing
func (f *S3IAMTestFramework) setupMockOIDCServer() {

	f.mockOIDC = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid_configuration":
			config := map[string]interface{}{
				"issuer":            "http://" + r.Host,
				"jwks_uri":          "http://" + r.Host + "/jwks",
				"userinfo_endpoint": "http://" + r.Host + "/userinfo",
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{
				"issuer": "%s",
				"jwks_uri": "%s",
				"userinfo_endpoint": "%s"
			}`, config["issuer"], config["jwks_uri"], config["userinfo_endpoint"])

		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{
				"keys": [
					{
						"kty": "RSA",
						"kid": "test-key-id",
						"use": "sig",
						"alg": "RS256",
						"n": "%s",
						"e": "AQAB"
					}
				]
			}`, f.encodePublicKey())

		case "/userinfo":
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			token := strings.TrimPrefix(authHeader, "Bearer ")
			userInfo := map[string]interface{}{
				"sub":    "test-user",
				"email":  "test@example.com",
				"name":   "Test User",
				"groups": []string{"users", "developers"},
			}

			if strings.Contains(token, "admin") {
				userInfo["groups"] = []string{"admins"}
			}

			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{
				"sub": "%s",
				"email": "%s",
				"name": "%s",
				"groups": %v
			}`, userInfo["sub"], userInfo["email"], userInfo["name"], userInfo["groups"])

		default:
			http.NotFound(w, r)
		}
	}))
}

// encodePublicKey encodes the RSA public key for JWKS
func (f *S3IAMTestFramework) encodePublicKey() string {
	return base64.RawURLEncoding.EncodeToString(f.publicKey.N.Bytes())
}

// BearerTokenTransport is an HTTP transport that adds Bearer token authentication
type BearerTokenTransport struct {
	Transport http.RoundTripper
	Token     string
}

// RoundTrip implements the http.RoundTripper interface
func (t *BearerTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	newReq := req.Clone(req.Context())

	// Remove ALL existing Authorization headers first to prevent conflicts
	newReq.Header.Del("Authorization")
	newReq.Header.Del("X-Amz-Date")
	newReq.Header.Del("X-Amz-Content-Sha256")
	newReq.Header.Del("X-Amz-Signature")
	newReq.Header.Del("X-Amz-Algorithm")
	newReq.Header.Del("X-Amz-Credential")
	newReq.Header.Del("X-Amz-SignedHeaders")
	newReq.Header.Del("X-Amz-Security-Token")

	// Add Bearer token authorization header
	newReq.Header.Set("Authorization", "Bearer "+t.Token)

	// Extract and set the principal ARN from JWT token for security compliance
	if principal := t.extractPrincipalFromJWT(t.Token); principal != "" {
		newReq.Header.Set("X-SeaweedFS-Principal", principal)
	}

	// Token preview for logging (first 50 chars for security)
	tokenPreview := t.Token
	if len(tokenPreview) > 50 {
		tokenPreview = tokenPreview[:50] + "..."
	}

	// Use underlying transport
	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	return transport.RoundTrip(newReq)
}

// extractPrincipalFromJWT extracts the principal ARN from a JWT token without validating it
// This is used to set the X-SeaweedFS-Principal header that's required after our security fix
func (t *BearerTokenTransport) extractPrincipalFromJWT(tokenString string) string {
	// Parse the JWT token without validation to extract the principal claim
	token, _ := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// We don't validate the signature here, just extract the claims
		// This is safe because the actual validation happens server-side
		return []byte("dummy-key"), nil
	})

	// Even if parsing fails due to signature verification, we might still get claims
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		// Try multiple possible claim names for the principal ARN
		if principal, exists := claims["principal"]; exists {
			if principalStr, ok := principal.(string); ok {
				return principalStr
			}
		}
		if assumed, exists := claims["assumed"]; exists {
			if assumedStr, ok := assumed.(string); ok {
				return assumedStr
			}
		}
	}

	return ""
}

// generateSTSSessionToken creates a session token using the actual STS service for proper validation
func (f *S3IAMTestFramework) generateSTSSessionToken(username, roleName string, validDuration time.Duration, account string, customClaims map[string]interface{}) (string, error) {
	now := time.Now()
	signingKeyB64 := "dGVzdC1zaWduaW5nLWtleS0zMi1jaGFyYWN0ZXJzLWxvbmc="
	signingKey, err := base64.StdEncoding.DecodeString(signingKeyB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode signing key: %v", err)
	}

	// Generate a session ID that would be created by the STS service
	sessionId := fmt.Sprintf("test-session-%s-%s-%d", username, roleName, now.Unix())

	if account == "" {
		account = "123456789012" // Default test account
	}

	// Create session token claims exactly matching STSSessionClaims struct
	roleArn := fmt.Sprintf("arn:aws:iam::%s:role/%s", account, roleName)
	sessionName := username
	principalArn := fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s/%s", account, roleName, sessionName)

	// Use jwt.MapClaims but with exact field names that STSSessionClaims expects
	sessionClaims := jwt.MapClaims{
		// RegisteredClaims fields
		"iss": "seaweedfs-sts",
		"sub": sessionId,
		"iat": now.Unix(),
		"exp": now.Add(validDuration).Unix(),
		"nbf": now.Unix(),

		// STSSessionClaims fields (using exact JSON tags from the struct)
		"sid":        sessionId,                      // SessionId
		"snam":       sessionName,                    // SessionName
		"typ":        "session",                      // TokenType
		"role":       roleArn,                        // RoleArn
		"assumed":    principalArn,                   // AssumedRole
		"principal":  principalArn,                   // Principal
		"idp":        "test-oidc",                    // IdentityProvider
		"ext_uid":    username,                       // ExternalUserId
		"assumed_at": now.Format(time.RFC3339Nano),   // AssumedAt
		"max_dur":    int64(validDuration.Seconds()), // MaxDuration
	}

	// Add custom claims (e.g., for ldap:* or jwt:* testing)
	for k, v := range customClaims {
		sessionClaims[k] = v
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, sessionClaims)
	tokenString, err := token.SignedString(signingKey)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

// CreateS3ClientWithJWT creates an S3 client authenticated with a JWT token for the specified role
func (f *S3IAMTestFramework) CreateS3ClientWithJWT(username, roleName string) (*s3.S3, error) {
	return f.CreateS3ClientWithCustomClaims(username, roleName, "", nil)
}

// CreateS3ClientWithCustomClaims creates an S3 client with specific account ID and custom claims
func (f *S3IAMTestFramework) CreateS3ClientWithCustomClaims(username, roleName, account string, claims map[string]interface{}) (*s3.S3, error) {
	var token string
	var err error

	if f.useKeycloak && claims == nil && account == "" {
		// Use real Keycloak authentication if no custom requirements
		token, err = f.getKeycloakToken(username)
		if err != nil {
			return nil, fmt.Errorf("failed to get Keycloak token: %v", err)
		}
	} else {
		// Generate STS session token (mock mode or custom requirements)
		token, err = f.generateSTSSessionToken(username, roleName, time.Hour, account, claims)
		if err != nil {
			return nil, fmt.Errorf("failed to generate STS session token: %v", err)
		}
	}

	// Create custom HTTP client with Bearer token transport
	httpClient := &http.Client{
		Transport: &BearerTokenTransport{
			Token: token,
		},
	}

	sess, err := session.NewSession(&aws.Config{
		Region:     aws.String(TestRegion),
		Endpoint:   aws.String(TestS3Endpoint),
		HTTPClient: httpClient,
		// Use anonymous credentials to avoid AWS signature generation
		Credentials:      credentials.AnonymousCredentials,
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	return s3.New(sess), nil
}

// CreateS3ClientWithInvalidJWT creates an S3 client with an invalid JWT token
func (f *S3IAMTestFramework) CreateS3ClientWithInvalidJWT() (*s3.S3, error) {
	invalidToken := "invalid.jwt.token"

	// Create custom HTTP client with Bearer token transport
	httpClient := &http.Client{
		Transport: &BearerTokenTransport{
			Token: invalidToken,
		},
	}

	sess, err := session.NewSession(&aws.Config{
		Region:     aws.String(TestRegion),
		Endpoint:   aws.String(TestS3Endpoint),
		HTTPClient: httpClient,
		// Use anonymous credentials to avoid AWS signature generation
		Credentials:      credentials.AnonymousCredentials,
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	return s3.New(sess), nil
}

// CreateS3ClientWithExpiredJWT creates an S3 client with an expired JWT token
func (f *S3IAMTestFramework) CreateS3ClientWithExpiredJWT(username, roleName string) (*s3.S3, error) {
	// Generate expired STS session token (expired 1 hour ago)
	token, err := f.generateSTSSessionToken(username, roleName, -time.Hour, "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to generate expired STS session token: %v", err)
	}

	// Create custom HTTP client with Bearer token transport
	httpClient := &http.Client{
		Transport: &BearerTokenTransport{
			Token: token,
		},
	}

	sess, err := session.NewSession(&aws.Config{
		Region:     aws.String(TestRegion),
		Endpoint:   aws.String(TestS3Endpoint),
		HTTPClient: httpClient,
		// Use anonymous credentials to avoid AWS signature generation
		Credentials:      credentials.AnonymousCredentials,
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	return s3.New(sess), nil
}

// CreateS3ClientWithSessionToken creates an S3 client with a session token
func (f *S3IAMTestFramework) CreateS3ClientWithSessionToken(sessionToken string) (*s3.S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String(TestRegion),
		Endpoint: aws.String(TestS3Endpoint),
		Credentials: credentials.NewStaticCredentials(
			"session-access-key",
			"session-secret-key",
			sessionToken,
		),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	return s3.New(sess), nil
}

// CreateS3ClientWithKeycloakToken creates an S3 client using a Keycloak JWT token
func (f *S3IAMTestFramework) CreateS3ClientWithKeycloakToken(keycloakToken string) (*s3.S3, error) {
	// Determine response header timeout based on environment
	responseHeaderTimeout := 10 * time.Second
	overallTimeout := 30 * time.Second
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		responseHeaderTimeout = 30 * time.Second // Longer timeout for CI JWT validation
		overallTimeout = 60 * time.Second
	}

	// Create a fresh HTTP transport with appropriate timeouts
	transport := &http.Transport{
		DisableKeepAlives:     true, // Force new connections for each request
		DisableCompression:    true, // Disable compression to simplify requests
		MaxIdleConns:          0,    // No connection pooling
		MaxIdleConnsPerHost:   0,    // No connection pooling per host
		IdleConnTimeout:       1 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout, // Adjustable for CI environments
		ExpectContinueTimeout: 1 * time.Second,
	}

	// Create a custom HTTP client with appropriate timeouts
	httpClient := &http.Client{
		Timeout: overallTimeout, // Overall request timeout (adjustable for CI)
		Transport: &BearerTokenTransport{
			Token:     keycloakToken,
			Transport: transport,
		},
	}

	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(TestRegion),
		Endpoint:         aws.String(TestS3Endpoint),
		Credentials:      credentials.AnonymousCredentials,
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
		MaxRetries:       aws.Int(0), // No retries to avoid delays
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %v", err)
	}

	return s3.New(sess), nil
}

// TestKeycloakTokenDirectly tests a Keycloak token with direct HTTP request (bypassing AWS SDK)
func (f *S3IAMTestFramework) TestKeycloakTokenDirectly(keycloakToken string) error {
	// Create a simple HTTP client with timeout
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Create request to list buckets
	req, err := http.NewRequest("GET", TestS3Endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Add Bearer token
	req.Header.Set("Authorization", "Bearer "+keycloakToken)
	req.Header.Set("Host", "localhost:8333")

	// Make request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Read response
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	return nil
}

// generateJWTToken creates a JWT token for testing
func (f *S3IAMTestFramework) generateJWTToken(username, roleName string, validDuration time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":   username,
		"iss":   f.mockOIDC.URL,
		"aud":   "test-client",
		"exp":   now.Add(validDuration).Unix(),
		"iat":   now.Unix(),
		"email": username + "@example.com",
		"name":  strings.Title(username),
	}

	// Add role-specific groups
	switch roleName {
	case "TestAdminRole":
		claims["groups"] = []string{"admins"}
	case "TestReadOnlyRole":
		claims["groups"] = []string{"users"}
	case "TestWriteOnlyRole":
		claims["groups"] = []string{"writers"}
	default:
		claims["groups"] = []string{"users"}
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key-id"

	tokenString, err := token.SignedString(f.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %v", err)
	}

	return tokenString, nil
}

// CreateShortLivedSessionToken creates a mock session token for testing
func (f *S3IAMTestFramework) CreateShortLivedSessionToken(username, roleName string, durationSeconds int64) (string, error) {
	// For testing purposes, create a mock session token
	// In reality, this would be generated by the STS service
	return fmt.Sprintf("mock-session-token-%s-%s-%d", username, roleName, time.Now().Unix()), nil
}

// ExpireSessionForTesting simulates session expiration for testing
func (f *S3IAMTestFramework) ExpireSessionForTesting(sessionToken string) error {
	// For integration tests, this would typically involve calling the STS service
	// For now, we just simulate success since the actual expiration will be handled by SeaweedFS
	return nil
}

// GenerateUniqueBucketName generates a unique bucket name for testing
func (f *S3IAMTestFramework) GenerateUniqueBucketName(prefix string) string {
	// Use test name and timestamp to ensure uniqueness
	testName := strings.ToLower(f.t.Name())
	testName = strings.ReplaceAll(testName, "/", "-")
	testName = strings.ReplaceAll(testName, "_", "-")

	// Truncate test name to keep total length under 63 characters
	// S3 bucket names must be 3-63 characters, lowercase, no underscores
	// Format: prefix-testname-random (need room for random suffix)
	maxTestNameLen := 63 - len(prefix) - 5 - 4 // account for dashes and random suffix
	if len(testName) > maxTestNameLen {
		testName = testName[:maxTestNameLen]
	}

	// Add random suffix to handle parallel tests
	randomSuffix := mathrand.Intn(10000)

	bucketName := fmt.Sprintf("%s-%s-%d", prefix, testName, randomSuffix)
	
	// Ensure final name is valid
	if len(bucketName) > 63 {
		// Truncate further if necessary
		bucketName = bucketName[:63]
	}
	
	return bucketName
}

// CreateBucket creates a bucket and tracks it for cleanup
func (f *S3IAMTestFramework) CreateBucket(s3Client *s3.S3, bucketName string) error {
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	// Track bucket for cleanup
	f.createdBuckets = append(f.createdBuckets, bucketName)
	return nil
}

// CreateBucketWithCleanup creates a bucket, cleaning up any existing bucket first
func (f *S3IAMTestFramework) CreateBucketWithCleanup(s3Client *s3.S3, bucketName string) error {
	// First try to create the bucket normally
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		// If bucket already exists, clean it up first
		if awsErr, ok := err.(awserr.Error); ok && (awsErr.Code() == "BucketAlreadyExists" || awsErr.Code() == "BucketAlreadyOwnedByYou") {
			f.t.Logf("Bucket %s already exists, cleaning up first", bucketName)

			// First try to delete the bucket completely
			f.emptyBucket(s3Client, bucketName)
			_, deleteErr := s3Client.DeleteBucket(&s3.DeleteBucketInput{
				Bucket: aws.String(bucketName),
			})
			if deleteErr != nil {
				f.t.Logf("Warning: Failed to delete existing bucket %s: %v", bucketName, deleteErr)
			}

			// Add a small delay to allow deletion to propagate
			time.Sleep(100 * time.Millisecond)

			// Now create it fresh
			_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				// If it still says bucket exists after cleanup, it might be in an inconsistent state
				// In this case, just use the existing bucket since we emptied it
				if awsErr, ok := err.(awserr.Error); ok && (awsErr.Code() == "BucketAlreadyExists" || awsErr.Code() == "BucketAlreadyOwnedByYou") {
					f.t.Logf("Bucket %s still exists after cleanup, reusing it", bucketName)
					// Bucket exists and is empty, so we can proceed
				} else {
					return fmt.Errorf("failed to recreate bucket after cleanup: %v", err)
				}
			}
		} else {
			return err
		}
	}

	// Track bucket for cleanup
	f.createdBuckets = append(f.createdBuckets, bucketName)
	return nil
}

// emptyBucket removes all objects from a bucket
func (f *S3IAMTestFramework) emptyBucket(s3Client *s3.S3, bucketName string) {
	// Delete all objects
	listResult, err := s3Client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		for _, obj := range listResult.Contents {
			_, err := s3Client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
			if err != nil {
				f.t.Logf("Warning: Failed to delete object %s/%s: %v", bucketName, *obj.Key, err)
			}
		}
	}
}

// Cleanup cleans up test resources
func (f *S3IAMTestFramework) Cleanup() {
	// Clean up buckets (best effort)
	if len(f.createdBuckets) > 0 {
		// Create admin client for cleanup
		adminClient, err := f.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		if err == nil {
			for _, bucket := range f.createdBuckets {
				// Try to empty bucket first
				listResult, err := adminClient.ListObjects(&s3.ListObjectsInput{
					Bucket: aws.String(bucket),
				})
				if err == nil {
					for _, obj := range listResult.Contents {
						adminClient.DeleteObject(&s3.DeleteObjectInput{
							Bucket: aws.String(bucket),
							Key:    obj.Key,
						})
					}
				}

				// Delete bucket
				adminClient.DeleteBucket(&s3.DeleteBucketInput{
					Bucket: aws.String(bucket),
				})
			}
		}
	}

	// Close mock OIDC server
	if f.mockOIDC != nil {
		f.mockOIDC.Close()
	}
}

// WaitForS3Service waits for the S3 service to be available
func (f *S3IAMTestFramework) WaitForS3Service() error {
	// Create a basic S3 client
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String(TestRegion),
		Endpoint: aws.String(TestS3Endpoint),
		Credentials: credentials.NewStaticCredentials(
			"test-access-key",
			"test-secret-key",
			"",
		),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %v", err)
	}

	s3Client := s3.New(sess)

	// Try to list buckets to check if service is available
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		_, err := s3Client.ListBuckets(&s3.ListBucketsInput{})
		if err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("S3 service not available after %d retries", maxRetries)
}

// PutTestObject puts a test object in the specified bucket
func (f *S3IAMTestFramework) PutTestObject(client *s3.S3, bucket, key, content string) error {
	_, err := client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	return err
}

// GetTestObject retrieves a test object from the specified bucket
func (f *S3IAMTestFramework) GetTestObject(client *s3.S3, bucket, key string) (string, error) {
	result, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	defer result.Body.Close()

	content := strings.Builder{}
	_, err = io.Copy(&content, result.Body)
	if err != nil {
		return "", err
	}

	return content.String(), nil
}

// ListTestObjects lists objects in the specified bucket
func (f *S3IAMTestFramework) ListTestObjects(client *s3.S3, bucket string) ([]string, error) {
	result, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, obj := range result.Contents {
		keys = append(keys, *obj.Key)
	}

	return keys, nil
}

// DeleteTestObject deletes a test object from the specified bucket
func (f *S3IAMTestFramework) DeleteTestObject(client *s3.S3, bucket, key string) error {
	_, err := client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// WaitForS3Service waits for the S3 service to be available (simplified version)
func (f *S3IAMTestFramework) WaitForS3ServiceSimple() error {
	// This is a simplified version that just checks if the endpoint responds
	// The full implementation would be in the Makefile's wait-for-services target
	return nil
}
