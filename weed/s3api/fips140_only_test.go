package s3api

import (
	"bytes"
	"crypto/fips140"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// setFIPS140Enforced overrides fips140Enforced for the duration of the test.
func setFIPS140Enforced(t *testing.T, enforced bool) {
	t.Helper()
	saved := fips140Enforced
	fips140Enforced = func() bool { return enforced }
	t.Cleanup(func() { fips140Enforced = saved })
}

func signedV2Request(t *testing.T, cred *Credential) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("Authorization", signatureV2(cred, req.Method, req.URL.EscapedPath(), req.URL.Query().Encode(), req.Header))
	return req
}

func presignedV2Request(t *testing.T, cred *Credential) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	expires := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	query := req.URL.Query()
	query.Set("AWSAccessKeyId", cred.AccessKey)
	query.Set("Expires", expires)
	query.Set("Signature", preSignatureV2(cred, req.Method, req.URL.EscapedPath(), query.Encode(), req.Header, expires))
	req.URL.RawQuery = query.Encode()
	return req
}

func policyV2Form(cred *Credential) http.Header {
	policy := base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T00:00:00Z","conditions":[]}`))
	form := http.Header{}
	form.Set("AWSAccessKeyId", cred.AccessKey)
	form.Set("Policy", policy)
	form.Set("Signature", calculateSignatureV2(policy, cred.SecretKey))
	return form
}

func TestFIPS140OnlySigV2(t *testing.T) {
	iam := setupTestIAMForV2Auth()
	cred := &Credential{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	signed := signedV2Request(t, cred)
	presigned := presignedV2Request(t, cred)
	form := policyV2Form(cred)

	for _, enforced := range []bool{false, true} {
		t.Run("enforced="+strconv.FormatBool(enforced), func(t *testing.T) {
			setFIPS140Enforced(t, enforced)
			want := s3err.ErrNone
			if enforced {
				want = s3err.ErrSignatureVersionNotSupported
			}
			if _, code := iam.isReqAuthenticatedV2(signed); code != want {
				t.Errorf("header SigV2: got %v, want %v", code, want)
			}
			if _, code := iam.isReqAuthenticatedV2(presigned); code != want {
				t.Errorf("presigned SigV2: got %v, want %v", code, want)
			}
			if _, code := iam.doesPolicySignatureV2Match(t.Context(), form); code != want {
				t.Errorf("POST policy SigV2: got %v, want %v", code, want)
			}
		})
	}
}

func TestFIPS140OnlySSEC(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, SSECustomerKeySize)
	keyMD5 := md5.Sum(key)
	keyB64 := base64.StdEncoding.EncodeToString(key)
	keyMD5B64 := base64.StdEncoding.EncodeToString(keyMD5[:])

	ssecHeaders := http.Header{}
	ssecHeaders.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	ssecHeaders.Set(s3_constants.AmzServerSideEncryptionCustomerKey, keyB64)
	ssecHeaders.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5B64)

	copySourceHeaders := http.Header{}
	copySourceHeaders.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm, "AES256")
	copySourceHeaders.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerKey, keyB64)
	copySourceHeaders.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerKeyMD5, keyMD5B64)

	sses3Headers := http.Header{}
	sses3Headers.Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmAES256)

	t.Run("enforced", func(t *testing.T) {
		setFIPS140Enforced(t, true)
		if _, err := ParseSSECHeaders(&http.Request{Header: ssecHeaders}); MapSSECErrorToS3Error(err) != s3err.ErrNotImplemented {
			t.Errorf("ParseSSECHeaders: got %v, want NotImplemented", err)
		}
		if _, err := ParseSSECCopySourceHeaders(&http.Request{Header: copySourceHeaders}); MapSSECErrorToS3Error(err) != s3err.ErrNotImplemented {
			t.Errorf("ParseSSECCopySourceHeaders: got %v, want NotImplemented", err)
		}
		if k, err := ParseSSECHeaders(&http.Request{Header: http.Header{}}); k != nil || err != nil {
			t.Errorf("no SSE-C headers: got %v, %v, want nil, nil", k, err)
		}
		for name, h := range map[string]http.Header{"SSE-C": ssecHeaders, "copy source SSE-C": copySourceHeaders} {
			if code := ValidateRequestEncryption(h); code != s3err.ErrNotImplemented {
				t.Errorf("ValidateRequestEncryption(%s): got %v, want NotImplemented", name, code)
			}
		}
		if code := ValidateRequestEncryption(sses3Headers); code != s3err.ErrNone {
			t.Errorf("ValidateRequestEncryption(SSE-S3): got %v, want ErrNone", code)
		}
	})

	t.Run("not enforced", func(t *testing.T) {
		setFIPS140Enforced(t, false)
		k, err := ParseSSECHeaders(&http.Request{Header: ssecHeaders})
		if err != nil || k == nil || !bytes.Equal(k.Key, key) {
			t.Errorf("ParseSSECHeaders: got %v, %v, want the parsed key", k, err)
		}
		for name, h := range map[string]http.Header{"SSE-C": ssecHeaders, "copy source SSE-C": copySourceHeaders, "SSE-S3": sses3Headers} {
			if code := ValidateRequestEncryption(h); code != s3err.ErrNone {
				t.Errorf("ValidateRequestEncryption(%s): got %v, want ErrNone", name, code)
			}
		}
	})
}

func TestFIPS140OnlySHA1Checksum(t *testing.T) {
	sha1Requests := map[string]func(r *http.Request){
		"value header":         func(r *http.Request) { r.Header.Set(s3_constants.AmzChecksumSHA1, "2jmj7l5rSw0yVb/vlWAYkK/YBwk=") },
		"sdk algorithm header": func(r *http.Request) { r.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "SHA1") },
		"algorithm header":     func(r *http.Request) { r.Header.Set(s3_constants.AmzChecksumAlgorithm, "sha1") },
		"trailer":              func(r *http.Request) { r.Header.Set(s3_constants.AmzTrailer, "x-amz-checksum-sha1") },
		"presigned query": func(r *http.Request) {
			r.URL.RawQuery = url.Values{s3_constants.AmzSdkChecksumAlgorithm: {"SHA1"}}.Encode()
		},
	}
	newRequest := func(setup func(r *http.Request)) *http.Request {
		r, _ := http.NewRequest(http.MethodPut, "http://example.com/bucket/object", nil)
		setup(r)
		return r
	}

	t.Run("enforced", func(t *testing.T) {
		setFIPS140Enforced(t, true)
		for name, setup := range sha1Requests {
			if alg, _, code := detectRequestedChecksumAlgorithm(newRequest(setup)); alg != ChecksumAlgorithmNone || code != s3err.ErrNotImplemented {
				t.Errorf("%s: got %v, %v, want None, NotImplemented", name, alg, code)
			}
		}
		sha256 := newRequest(func(r *http.Request) { r.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "SHA256") })
		if alg, _, code := detectRequestedChecksumAlgorithm(sha256); alg != ChecksumAlgorithmSHA256 || code != s3err.ErrNone {
			t.Errorf("SHA256: got %v, %v, want SHA256, ErrNone", alg, code)
		}
	})

	t.Run("not enforced", func(t *testing.T) {
		setFIPS140Enforced(t, false)
		for name, setup := range sha1Requests {
			if alg, header, code := detectRequestedChecksumAlgorithm(newRequest(setup)); alg != ChecksumAlgorithmSHA1 || header != s3_constants.AmzChecksumSHA1 || code != s3err.ErrNone {
				t.Errorf("%s: got %v, %q, %v, want SHA1, %q, ErrNone", name, alg, header, code, s3_constants.AmzChecksumSHA1)
			}
		}
	})
}

func TestFIPS140OnlySHA1ChunkedTrailer(t *testing.T) {
	newRequest := func() *http.Request {
		r, _ := http.NewRequest(http.MethodPut, "/test-bucket/test-object", bytes.NewReader(nil))
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		r.Header.Set("x-amz-trailer", "x-amz-checksum-sha1")
		return r
	}
	iam := setupTestIAM()

	for _, enforced := range []bool{false, true} {
		t.Run("enforced="+strconv.FormatBool(enforced), func(t *testing.T) {
			setFIPS140Enforced(t, enforced)
			want := s3err.ErrNone
			if enforced {
				want = s3err.ErrNotImplemented
			}
			if _, code := iam.newChunkedReader(newRequest()); code != want {
				t.Errorf("got %v, want %v", code, want)
			}
		})
	}
}

// policyV4Form is a POST policy form signed with SigV4 by cred.
func policyV4Form(cred *Credential) http.Header {
	now := time.Now().UTC()
	policy := base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T00:00:00Z","conditions":[]}`))
	form := http.Header{}
	form.Set("X-Amz-Credential", fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", cred.AccessKey, now.Format(yyyymmdd)))
	form.Set("X-Amz-Date", now.Format(iso8601Format))
	form.Set("Policy", policy)
	form.Set("X-Amz-Signature", getSignature(getSigningKey(cred.SecretKey, now.Format(yyyymmdd), "us-east-1", "s3"), policy))
	return form
}

func TestFIPS140OnlyShortSecret(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("GODEBUG=fips140=only panics signing the fixtures with a 9-character secret")
	}
	short := &Credential{AccessKey: "AKIASHORTSECRET0001", SecretKey: "abcd12345"}
	ten := &Credential{AccessKey: "AKIATENSECRET000001", SecretKey: "abcd123456"}
	iam := &IdentityAccessManagement{accessKeyIdent: map[string]*Identity{}}
	for _, cred := range []*Credential{short, ten} {
		ident := &Identity{Name: cred.AccessKey, Credentials: []*Credential{cred}, Actions: []Action{s3_constants.ACTION_ADMIN}}
		iam.identities = append(iam.identities, ident)
		iam.accessKeyIdent[cred.AccessKey] = ident
	}
	signed := func(cred *Credential) *http.Request {
		req := mustNewRequest(http.MethodGet, "http://example.com/bucket/object", 0, nil, t)
		if err := signRequestV4(req, cred.AccessKey, cred.SecretKey); err != nil {
			t.Fatal(err)
		}
		return req
	}
	presigned := func(cred *Credential) *http.Request {
		req := mustNewRequest(http.MethodGet, "http://example.com/bucket/object", 0, nil, t)
		if err := preSignV4(iam, req, cred.AccessKey, cred.SecretKey, 600); err != nil {
			t.Fatal(err)
		}
		return req
	}

	for _, enforced := range []bool{false, true} {
		t.Run("enforced="+strconv.FormatBool(enforced), func(t *testing.T) {
			setFIPS140Enforced(t, enforced)
			for _, cred := range []*Credential{short, ten} {
				want := s3err.ErrNone
				if enforced && cred == short {
					want = s3err.ErrSignatureDoesNotMatch
				}
				if _, _, code := iam.doesSignatureMatch(signed(cred)); code != want {
					t.Errorf("header SigV4 with a %d-character secret: got %v, want %v", len(cred.SecretKey), code, want)
				}
				if _, _, code := iam.doesSignatureMatch(presigned(cred)); code != want {
					t.Errorf("presigned SigV4 with a %d-character secret: got %v, want %v", len(cred.SecretKey), code, want)
				}
				if _, code := iam.doesPolicySignatureV4Match(t.Context(), policyV4Form(cred)); code != want {
					t.Errorf("POST policy SigV4 with a %d-character secret: got %v, want %v", len(cred.SecretKey), code, want)
				}
			}
		})
	}
}

// TestContentMd5CheckedInBothDigests checks that a client's Content-MD5 is
// verified against the body's real MD5 under digest md5 and digest sha256,
// and goes unchecked only when no MD5 can be computed.
func TestContentMd5CheckedInBothDigests(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("GODEBUG=fips140=only refuses MD5")
	}
	t.Cleanup(func() { util.SetDigest("") })
	body := []byte("hello")
	right := md5.Sum(body)
	wrong := md5.Sum([]byte("other"))
	for _, digest := range []string{util.DigestMd5, util.DigestSha256} {
		if err := util.SetDigest(digest); err != nil {
			t.Fatal(err)
		}
		contentMd5Hash := util.NewContentMd5()
		if contentMd5Hash != nil {
			contentMd5Hash.Write(body)
		}
		bodyMd5 := util.BodyMd5(util.Md5(body), contentMd5Hash)
		if !contentMd5Matches(bodyMd5, right[:]) {
			t.Errorf("digest %s: the right Content-MD5 was refused", digest)
		}
		if contentMd5Matches(bodyMd5, wrong[:]) {
			t.Errorf("digest %s: a wrong Content-MD5 matched", digest)
		}
	}
	if !contentMd5Matches(nil, wrong[:]) {
		t.Error("without a body MD5 Content-MD5 should go unchecked")
	}
}
