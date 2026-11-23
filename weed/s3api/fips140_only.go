package s3api

import (
	"bytes"
	"crypto/fips140"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// fips140Enforced reports whether the process runs with GODEBUG=fips140=only.
// In that mode MD5, SHA-1 and HMAC-SHA1 panic, so SigV2, SSE-C and the SHA-1
// flexible checksum are refused with an S3 error before any hashing.
// It is a variable so tests can enable the refusals without running the whole
// test binary under fips140=only.
var fips140Enforced = fips140.Enforced

// contentMd5Matches compares a body digest with the client's Content-MD5.
// Under fips140=only the digest is not an MD5, so Content-MD5 goes unchecked.
func contentMd5Matches(digest, contentMd5 []byte) bool {
	return fips140Enforced() || bytes.Equal(digest, contentMd5)
}

// fips140SecretTooShort reports whether SigV4 cannot sign with secretKey under
// fips140=only, where hmac.New panics on the short "AWS4"+secret key.
func fips140SecretTooShort(secretKey string) bool {
	return fips140Enforced() && len(secretKey) < iamlib.Fips140MinSecretAccessKeyLength
}

// warnFips140ShortSecrets logs each access key in config whose secret SigV4
// cannot use under fips140=only. The credentials stay loaded and stored;
// requests signed with them get InvalidAccessKeyId.
func warnFips140ShortSecrets(config *iam_pb.S3ApiConfiguration) {
	if !fips140Enforced() {
		return
	}
	warn := func(owner string, cred *iam_pb.Credential) {
		if cred != nil && fips140SecretTooShort(cred.SecretKey) {
			glog.Warningf("access key %s of %s: secret is shorter than %d characters, which SigV4 cannot use under GODEBUG=fips140=only; requests signed with it are refused",
				cred.AccessKey, owner, iamlib.Fips140MinSecretAccessKeyLength)
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

// refuseFips140ShortSecret answers ErrInvalidAccessKeyID, as for an inactive
// key, when cred's secret is too short for SigV4 under fips140=only, where
// deriving the signing key from it would panic.
func refuseFips140ShortSecret(cred *Credential) s3err.ErrorCode {
	if !fips140SecretTooShort(cred.SecretKey) {
		return s3err.ErrNone
	}
	glog.Warningf("InvalidAccessKeyId: access key %s has a secret shorter than %d characters, which SigV4 cannot use under GODEBUG=fips140=only",
		cred.AccessKey, iamlib.Fips140MinSecretAccessKeyLength)
	return s3err.ErrInvalidAccessKeyID
}
