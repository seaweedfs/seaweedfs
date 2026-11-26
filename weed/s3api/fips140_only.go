package s3api

import (
	"bytes"
	"context"
	"crypto/fips140"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

// fips140Enforced reports whether the process runs with GODEBUG=fips140=only.
// In that mode MD5, SHA-1 and HMAC-SHA1 panic, so SigV2, SSE-C and the SHA-1
// flexible checksum are refused with an S3 error before any hashing.
// It is a variable so tests can enable the refusals without running the whole
// test binary under fips140=only.
var fips140Enforced = fips140.Enforced

// contentMd5Matches compares the body's MD5, from util.BodyMd5, with the
// client's Content-MD5. bodyMd5 is nil only under fips140=only with digest
// sha256, where no MD5 can be computed, and Content-MD5 then goes unchecked.
func contentMd5Matches(bodyMd5, contentMd5 []byte) bool {
	return bodyMd5 == nil || bytes.Equal(bodyMd5, contentMd5)
}

// fips140SecretTooShort reports whether SigV4 cannot sign with secretKey under
// fips140=only, where hmac.New panics on the short "AWS4"+secret key.
func fips140SecretTooShort(secretKey string) bool {
	return fips140Enforced() && len(secretKey) < iamlib.Fips140MinSecretAccessKeyLength
}

// warnFips140ShortSecrets logs each access key in config whose secret SigV4
// cannot use under fips140=only. The credentials stay loaded and stored;
// requests signed with them get SignatureDoesNotMatch.
func warnFips140ShortSecrets(config *iam_pb.S3ApiConfiguration) {
	if !fips140Enforced() {
		return
	}
	warn := func(owner string, cred *iam_pb.Credential) {
		if cred != nil && fips140SecretTooShort(cred.SecretKey) {
			glog.Errorf("this process runs with GODEBUG=fips140=only, and refuses all S3 requests signed with access key %s (%s) because its secret is shorter than the %d characters FIPS 140 needs for SigV4 secrets; give that access key a new secret of at least %d characters",
				cred.AccessKey, owner, iamlib.Fips140MinSecretAccessKeyLength, iamlib.Fips140MinSecretAccessKeyLength)
		}
	}
	for _, ident := range config.Identities {
		for _, cred := range ident.Credentials {
			warn("identity "+ident.Name, cred)
		}
	}
	for _, sa := range config.ServiceAccounts {
		warn("service account "+sa.Id, sa.Credential)
	}
}

// refuseFips140ShortSecret answers SignatureDoesNotMatch when cred's secret is
// too short for SigV4 under fips140=only, where deriving the signing key from it
// would panic. Callers run it where the signature would be checked, and the
// answer is the one a wrong signature gets, so a caller who knows the access key
// ID cannot tell that its secret is short. The log line gives the real cause,
// with the request ID the client gets back, the client's address when known,
// and the identity.
func refuseFips140ShortSecret(ctx context.Context, clientIP string, identity *Identity, cred *Credential) s3err.ErrorCode {
	if !fips140SecretTooShort(cred.SecretKey) {
		return s3err.ErrNone
	}
	from := ""
	if clientIP != "" {
		from = " from " + clientIP
	}
	owner := ""
	if identity != nil {
		owner = " (identity " + identity.Name + ")"
	}
	glog.Warningf("refused S3 request %s%s (SignatureDoesNotMatch): access key %s%s has a secret too short for SigV4 under FIPS 140; see the error logged when S3 loaded its identities",
		request_id.Get(ctx), from, cred.AccessKey, owner)
	return s3err.ErrSignatureDoesNotMatch
}
