package s3_backend

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	s3Sessions   = make(map[string]s3iface.S3API)
	sessionsLock sync.Mutex
)

// A tier endpoint that drops packets or accepts connections without answering
// must not hold a read, or a volume server's startup, for minutes per request.
var (
	s3ConnectTimeout        = 10 * time.Second
	s3ResponseHeaderTimeout = time.Minute
)

// httpOptions tunes the HTTP transport of the tier backend's S3 client.
type httpOptions struct {
	// disableHTTP2 keeps the client on HTTP/1.1. Over HTTP/2 every upload and
	// range read to the endpoint shares one connection and its flow-control
	// window, which some S3-compatible services keep small.
	disableHTTP2 bool
	// maxConnsPerHost caps the connections to the endpoint (0 = no cap), for
	// services that limit parallel connections per client address.
	maxConnsPerHost int
	// idleConnTimeout closes idle connections sooner than Go's default 90s,
	// for services that drop idle connections earlier (0 = default).
	idleConnTimeout time.Duration
}

func (o httpOptions) newTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: s3ConnectTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.ResponseHeaderTimeout = s3ResponseHeaderTimeout
	if o.disableHTTP2 {
		t.ForceAttemptHTTP2 = false
		t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	if o.maxConnsPerHost > 0 {
		t.MaxConnsPerHost = o.maxConnsPerHost
	}
	if o.idleConnTimeout > 0 {
		t.IdleConnTimeout = o.idleConnTimeout
	}
	return t
}

func createSession(awsAccessKeyId, awsSecretAccessKey, region, endpoint string, forcePathStyle bool, httpOpts httpOptions) (s3iface.S3API, error) {

	sessionsLock.Lock()
	defer sessionsLock.Unlock()

	cacheKey := fmt.Sprintf("%s|%s", region, endpoint)
	if httpOpts != (httpOptions{}) {
		cacheKey = fmt.Sprintf("%s|%+v", cacheKey, httpOpts)
	}
	if t, found := s3Sessions[cacheKey]; found {
		return t, nil
	}

	config := &aws.Config{
		Region:                        aws.String(region),
		Endpoint:                      aws.String(endpoint),
		S3ForcePathStyle:              aws.Bool(forcePathStyle),
		S3DisableContentMD5Validation: aws.Bool(true),
		HTTPClient:                    &http.Client{Transport: httpOpts.newTransport()},
	}
	if awsAccessKeyId != "" && awsSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aws session in region %s: %v", region, err)
	}
	sess.Handlers.Build.PushBack(func(r *request.Request) {
		r.HTTPRequest.Header.Set("User-Agent", "SeaweedFS/"+version.VERSION_NUMBER)
	})

	t := s3.New(sess)

	s3Sessions[cacheKey] = t

	return t, nil

}

func deleteFromS3(sess s3iface.S3API, sourceBucket string, sourceKey string) (err error) {
	_, err = sess.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(sourceKey),
	})
	return err
}
