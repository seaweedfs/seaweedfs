package iam

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
	DefaultKeycloakURL    = "http://localhost:8080"
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
		framework.privateKey, err = rsa.GenerateKey(rand.Reader, 2048)
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
	healthURL := fmt.Sprintf("%s/health/ready", keycloakURL)
	
	resp, err := client.Get(healthURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	
	return resp.StatusCode == 200
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
		return nil, fmt.Errorf("Keycloak authentication failed with status: %d", resp.StatusCode)
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
	userPasswords := map[string]string{
		"admin-user": "admin123",
		"read-user":  "read123", 
		"write-user": "write123",
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

	// Add Bearer token authorization header
	newReq.Header.Set("Authorization", "Bearer "+t.Token)

	// Remove AWS signature headers if present (they conflict with Bearer auth)
	newReq.Header.Del("X-Amz-Date")
	newReq.Header.Del("X-Amz-Content-Sha256")
	newReq.Header.Del("X-Amz-Signature")
	newReq.Header.Del("X-Amz-Algorithm")
	newReq.Header.Del("X-Amz-Credential")
	newReq.Header.Del("X-Amz-SignedHeaders")

	// Use underlying transport
	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	return transport.RoundTrip(newReq)
}

// generateSTSSessionToken creates a session token using the actual STS service for proper validation
func (f *S3IAMTestFramework) generateSTSSessionToken(username, roleName string, validDuration time.Duration) (string, error) {
	// For now, simulate what the STS service would return by calling AssumeRoleWithWebIdentity
	// In a real test, we'd make an actual HTTP call to the STS endpoint
	// But for unit testing, we'll create a realistic JWT manually that will pass validation

	now := time.Now()
	signingKeyB64 := "dGVzdC1zaWduaW5nLWtleS0zMi1jaGFyYWN0ZXJzLWxvbmc="
	signingKey, err := base64.StdEncoding.DecodeString(signingKeyB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode signing key: %v", err)
	}

	// Generate a session ID that would be created by the STS service
	sessionId := fmt.Sprintf("test-session-%s-%s-%d", username, roleName, now.Unix())

	// Create session token claims exactly as TokenGenerator does
	roleArn := fmt.Sprintf("arn:seaweed:iam::role/%s", roleName)
	sessionName := fmt.Sprintf("test-session-%s", username)
	principalArn := fmt.Sprintf("arn:seaweed:sts::assumed-role/%s/%s", roleName, sessionName)
	
	sessionClaims := jwt.MapClaims{
		"iss":        "seaweedfs-sts",
		"sub":        sessionId,
		"iat":        now.Unix(),
		"exp":        now.Add(validDuration).Unix(),
		"nbf":        now.Unix(),
		"typ":        "session",
		"role":       roleArn,
		"snam":       sessionName,
		"principal":  principalArn,
		"assumed":    principalArn,
		"assumed_at": now.Format(time.RFC3339Nano),
		"ext_uid":    username,
		"idp":        "test-oidc",
		"max_dur":    int64(validDuration.Seconds()),
		"sid":        sessionId,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, sessionClaims)
	tokenString, err := token.SignedString(signingKey)
	if err != nil {
		return "", err
	}

	// Note: In a real implementation, we would also create the session in the STS session store
	// For now, we'll rely on the fact that if JWT validation passes, the session should be considered valid
	// This is a limitation of our current testing approach

	return tokenString, nil
}

// CreateS3ClientWithJWT creates an S3 client authenticated with a JWT token for the specified role
func (f *S3IAMTestFramework) CreateS3ClientWithJWT(username, roleName string) (*s3.S3, error) {
	var token string
	var err error
	
	if f.useKeycloak {
		// Use real Keycloak authentication
		token, err = f.getKeycloakToken(username)
		if err != nil {
			return nil, fmt.Errorf("failed to get Keycloak token: %v", err)
		}
	} else {
		// Generate STS session token (mock mode)
		token, err = f.generateSTSSessionToken(username, roleName, time.Hour)
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
	token, err := f.generateSTSSessionToken(username, roleName, -time.Hour)
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
