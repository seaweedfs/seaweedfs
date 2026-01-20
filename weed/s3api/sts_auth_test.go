package s3api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// TestSTSAuthentication verifies STS signature validation logic,
// ensuring compliance with AWS STS signature rules (empty payload hash).
func TestSTSAuthentication(t *testing.T) {
	// Setup IAM - instantiate directly to avoid NewIdentityAccessManagement fatal errors in test env
    iam := &IdentityAccessManagement{
        domain:       "localhost",
        hashes:       make(map[string]*sync.Pool),
        hashCounters: make(map[string]*int32),
        identities:   make([]*Identity, 0),
        accessKeyIdent: make(map[string]*Identity),
        nameToIdentity: make(map[string]*Identity),
    }
	
	// Add admin user manually since we are using mock
	accessKey := "test-access-key"
	secretKey := "test-secret-key"
	
	validIdentity := &Identity{
		Name: "admin",
		Credentials: []*Credential{
			{
				AccessKey: accessKey,
				SecretKey: secretKey,
			},
		},
		Actions: []Action{s3_constants.ACTION_ADMIN},
	}
	
	iam.m.Lock()
	iam.identities = []*Identity{validIdentity}
	iam.accessKeyIdent = map[string]*Identity{accessKey: validIdentity}
	iam.isAuthEnabled = true
	iam.m.Unlock()
	
	// Helper to sign request manually
	signRequest := func(r *http.Request, service string, bodyContent []byte, signedHeaders map[string]string) {
		t := time.Now().UTC()
		dateStr := t.Format(iso8601Format)
		dateShort := t.Format(yyyymmdd)
		region := "us-east-1"
		
        // Calculate SHA256 of the body or empty string depending on service
        var bodyHashHex string
        if val, ok := signedHeaders["X-Amz-Content-Sha256"]; ok {
            bodyHashHex = val
        } else {
             // Default if not provided
             hash := sha256.Sum256(bodyContent)
             bodyHashHex = hex.EncodeToString(hash[:])
        }

		r.Header.Set("X-Amz-Date", dateStr)
        // Ensure Host header is always set
        r.Header.Set("Host", "localhost")
        
        // Add custom signed headers
        canonicalHeaders := fmt.Sprintf("content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:localhost\nx-amz-date:%s\n", dateStr)
        signedHeadersList := "content-type;host;x-amz-date"
        
        // Note: Map range iteration order is random, so for test stability we handle explicit known headers
        if val, ok := signedHeaders["X-Amz-Content-Sha256"]; ok {
            r.Header.Set("X-Amz-Content-Sha256", val)
            // AWS requires headers to be sorted by name in canonical request
            // x-amz-content-sha256 comes after host and before x-amz-date ? No, alphabetically: content-type, host, x-amz-content-sha256, x-amz-date
            
            // Re-build canonical headers correctly sorted
             canonicalHeaders = fmt.Sprintf("content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:localhost\nx-amz-content-sha256:%s\nx-amz-date:%s\n", val, dateStr)
            signedHeadersList = "content-type;host;x-amz-content-sha256;x-amz-date"
        }
		
		canonicalRequest := fmt.Sprintf("POST\n/\n\n%s\n%s\n%s", canonicalHeaders, signedHeadersList, bodyHashHex)
		
		scope := fmt.Sprintf("%s/%s/%s/aws4_request", dateShort, region, service)
		stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", dateStr, scope, getSHA256Hash([]byte(canonicalRequest)))
		
		kDate := sumHMAC([]byte("AWS4"+secretKey), []byte(dateShort))
		kRegion := sumHMAC(kDate, []byte(region))
		kService := sumHMAC(kRegion, []byte(service))
		kSigning := sumHMAC(kService, []byte("aws4_request"))
		
		signature := hex.EncodeToString(sumHMAC(kSigning, []byte(stringToSign)))
		
		authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", 
			accessKey, scope, signedHeadersList, signature)
			
		r.Header.Set("Authorization", authHeader)
	}

	// Test Case 1: Service = "s3" with Signed Payload (Standard S3)
	t.Run("Service S3 SignedPayload", func(t *testing.T) {
		body := []byte("Action=ListUsers&Version=2010-05-08")
		req := httptest.NewRequest("POST", "http://localhost/", bytes.NewReader(body))
        
        // S3 requires explicit payload hash header if we sign the body
		bodyHash := sha256.Sum256(body)
		bodyHashHex := hex.EncodeToString(bodyHash[:])
        
        signRequest(req, "s3", body, map[string]string{
            "X-Amz-Content-Sha256": bodyHashHex,
        })
        // Ensure Host header is set as it's part of canonical request
        req.Header.Set("Host", "localhost")
		
		identity, errCode := iam.Authenticate(req)
		assert.Equal(t, s3err.ErrNone, errCode)
		assert.NotNil(t, identity)
		if identity != nil {
			assert.Equal(t, "admin", identity.Name)
		}
	})

	// Test Case 2: Service = "sts" with Empty Payload Hash (Simulate AWS SDK/Boto3 behavior)
    // STS clients do NOT include payload hash in signature (hashedPayload = hash(""))
    // And they do NOT send X-Amz-Content-Sha256 header.
    // The server must default to empty payload hash for verification even if body is present.
	t.Run("Service STS EmptyHash", func(t *testing.T) {
		body := []byte("Action=GetCallerIdentity&Version=2011-06-15")
		req := httptest.NewRequest("POST", "http://localhost/", bytes.NewReader(body))
		
		// Sign with EMPTY hash (standard for STS)
		emptyHash := sha256.Sum256([]byte(""))
		emptyHashHex := hex.EncodeToString(emptyHash[:])
		
        // Pass empty hash to signer, but do NOT include X-Amz-Content-Sha256 header
        // This forces signRequest to sign the empty hash but rely on default headers logic
		tAttr := time.Now().UTC()
		dateStr := tAttr.Format(iso8601Format)
		dateShort := tAttr.Format(yyyymmdd)
		region := "us-east-1"
		service := "sts"
		
		req.Header.Set("X-Amz-Date", dateStr)
		req.Header.Set("Host", "localhost:8333")
        req.Host = "localhost:8333"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
        // No hash header
		
		canonicalRequest := fmt.Sprintf("POST\n/\n\ncontent-type:application/x-www-form-urlencoded; charset=utf-8\nhost:localhost:8333\nx-amz-date:%s\n\ncontent-type;host;x-amz-date\n%s", dateStr, emptyHashHex)
		
		scope := fmt.Sprintf("%s/%s/%s/aws4_request", dateShort, region, service)
		stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", dateStr, scope, getSHA256Hash([]byte(canonicalRequest)))
		
        // Calculate signature
		kDate := sumHMAC([]byte("AWS4"+secretKey), []byte(dateShort))
		kRegion := sumHMAC(kDate, []byte(region))
		kService := sumHMAC(kRegion, []byte(service))
		kSigning := sumHMAC(kService, []byte("aws4_request"))
		
		signature := hex.EncodeToString(sumHMAC(kSigning, []byte(stringToSign)))
        
		authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=content-type;host;x-amz-date, Signature=%s", 
			accessKey, scope, signature)
		req.Header.Set("Authorization", authHeader)
		
		identity, errCode := iam.Authenticate(req)
		
        if errCode != s3err.ErrNone {
             t.Logf("STS EmptyHash Authentication Test Failed: %v", errCode)
        }
		assert.Equal(t, s3err.ErrNone, errCode)
        assert.NotNil(t, identity)
	})
    
    // Test Case 3: Service = "iam" (Standard)
	t.Run("Service IAM", func(t *testing.T) {
		body := []byte("Action=ListUsers&Version=2010-05-08")
		req := httptest.NewRequest("POST", "http://localhost/", bytes.NewReader(body))
        
        // IAM usually behaves like S3 but let's check standard Boto3 behavior (ListUsers worked)
        // If ListUsers worked, it probably follows S3 conventions or STS-like. 
        // For now, testing generic signing.
		
        // We'll mimic S3 behavior for IAM test here as baseline
		bodyHash := sha256.Sum256(body)
		bodyHashHex := hex.EncodeToString(bodyHash[:])
        
        signRequest(req, "iam", body, map[string]string{
            "X-Amz-Content-Sha256": bodyHashHex,
        })
        // Ensure Host header is set
        req.Header.Set("Host", "localhost")
		
		identity, errCode := iam.Authenticate(req)
		assert.Equal(t, s3err.ErrNone, errCode)
		assert.NotNil(t, identity)
    })
}
