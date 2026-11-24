package s3

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
)

// WEED_REMOTE_S3_CREDENTIALS chooses where a remote of type "s3" with no
// access key and no secret in its configuration takes its credentials from:
//
//   - unset (the default): as before, the SDK default chain when the
//     environment names a credential source, else unsigned requests;
//   - "workload": the identity of the AWS workload only (web identity, the
//     ECS/EKS container credentials endpoint, the EC2 instance role), never
//     AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or a shared credentials file.
//     Every keyless "s3" remote of the process is signed with it, a public
//     bucket's too: set it only where the process runs as an AWS workload.
//
// The other S3-compatible types (wasabi, b2, filebase, storj, ...) are not
// affected. A remote with only one of the two keys is refused in workload
// mode: falling back to the default chain would sign with the environment's
// keys, which is what the setting rules out.
//
// "workload" exists because a SeaweedFS process often holds keys of its own in
// those variables: `weed mini` and `weed server -s3` read their initial S3
// identity from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY. On EC2, a remote
// mounted on an Amazon S3 bucket with the instance's role (no keys) then signed
// with that local identity and every call answered InvalidAccessKeyId, while a
// filer.remote.sync of the same remote, started without those variables,
// worked.
const workloadCredentialsEnv = "WEED_REMOTE_S3_CREDENTIALS"

// workloadCredentialsMode reports whether workload mode is on; an unknown
// value is an error, so a misspelling never falls back to the environment's
// keys without a word.
func workloadCredentialsMode() (bool, error) {
	switch v := strings.TrimSpace(os.Getenv(workloadCredentialsEnv)); v {
	case "":
		return false, nil
	case "workload":
		return true, nil
	default:
		return false, fmt.Errorf("%s=%q: the only value is \"workload\" (or unset)", workloadCredentialsEnv, v)
	}
}

// workloadCredentials is the workload's identity alone: web identity when the
// environment names a token file and a role (EKS IRSA), then the container
// credentials endpoint or the EC2 instance metadata service. The session it
// builds on carries anonymous credentials so that it resolves none of its own
// (a token file without a role would otherwise fail every build), and it
// fetches on an unguarded HTTP client: the guarded one is for the storage
// endpoint and refuses the link-local credential endpoints.
func workloadCredentials(region string) (*credentials.Credentials, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(region), HTTPClient: &http.Client{},
			Credentials: credentials.AnonymousCredentials},
		SharedConfigState: session.SharedConfigDisable,
	})
	if err != nil {
		return nil, err
	}
	var providers []credentials.Provider
	if tokenFile, role := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"), os.Getenv("AWS_ROLE_ARN"); tokenFile != "" && role != "" {
		name := os.Getenv("AWS_ROLE_SESSION_NAME")
		if name == "" {
			name = "seaweedfs-remote"
		}
		providers = append(providers, stscreds.NewWebIdentityRoleProviderWithOptions(
			sts.New(sess), role, name, stscreds.FetchTokenPath(tokenFile),
			func(p *stscreds.WebIdentityRoleProvider) { p.ExpiryWindow = 5 * time.Minute }))
	}
	providers = append(providers, defaults.RemoteCredProvider(*sess.Config, sess.Handlers))
	return credentials.NewCredentials(&credentials.ChainProvider{Providers: providers, VerboseErrors: true}), nil
}
