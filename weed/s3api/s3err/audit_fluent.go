package s3err

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	// "github.com/chrislusf/seaweedfs/weed/s3api/s3err"

	//"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"github.com/fluent/fluent-logger-golang/fluent"
	"net/http"
	"os"
	"time"
)

type AccessLogExtend  struct {
	AccessLog
	AccessLogHTTP
}

type AccessLog  struct {
	Bucket   string 		`json:"bucket"`	 // awsexamplebucket1
	Time	 time.Time		`json:"time"`		 // [06/Feb/2019:00:00:38 +0000]
	RemoteIP string			`json:"remote_ip,omitempty"` // 192.0.2.3
	Requester string		`json:"requester,omitempty"` // IAM user id
	RequestID string		`json:"request_id,omitempty"` // 3E57427F33A59F07
	Operation string		`json:"operation,omitempty"` // REST.HTTP_method.resource_type REST.PUT.OBJECT
	Key		  string 		`json:"Key,omitempty"`		 // /photos/2019/08/puppy.jpg
	ErrorCode string		`json:"error_code,omitempty"`
	HostId		string		`json:"host_id,omitempty"`
	HostHeader string 		`json:"host_header,omitempty"` // s3.us-west-2.amazonaws.com
	SignatureVersion string `json:"signature_version,omitempty"`
}

type AccessLogHTTP struct {
	RequestURI string		`json:"request_uri,omitempty"` // "GET /awsexamplebucket1/photos/2019/08/puppy.jpg?x-foo=bar HTTP/1.1"
	HTTPStatus int 			`json:"HTTP_status,omitempty"`
	BytesSent string		`json:"bytes_sent,omitempty"`
	ObjectSize string		`json:"object_size,omitempty"`
	TotalTime time.Duration `json:"total_time,omitempty"`
	TurnAroundTime time.Duration `json:"turn_around_time,omitempty"`
	Referer string 			`json:"Referer,omitempty"`
	UserAgent string 		`json:"user_agent,omitempty"`
	VersionId	string		`json:"version_id,omitempty"`
	CipherSuite string 		`json:"cipher_suite,omitempty"`
	AuthenticationType string `json:"auth_type,omitempty"`
	TLSVersion	string		`json:"TLS_version,omitempty"`
}

const tag = "s3.access"

var (
	logger  *fluent.Fluent
	hostname = os.Getenv("HOSTNAME")
)

func init() {
	var err error
	logger, err = fluent.New(fluent.Config{})
	if err != nil {
		glog.Fatalf("fail to load fluent config: %v", err)
	}
}

func getREST(httpMetod string, resourceType string) string {
	return fmt.Sprintf("REST.%s.%s", httpMetod, resourceType)
}

func getResourceType(object string, query string, metod string) (string, bool) {
	if len(object) > 0 {
		switch query {
		case "tagging":
			return getREST(metod, "OBJECTTAGGING"), true
		default:
			return getREST(metod, "OBJECT"), false
		}
	} else {
		switch query {
		case "delete":
			return "BATCH.DELETE.OBJECT", true
		case "tagging":
			return getREST(metod, "OBJECTTAGGING"), true
		case "lifecycle":
			return getREST(metod, "LIFECYCLECONFIGURATION"), true
		case "acl":
			return getREST(metod, "ACCESSCONTROLPOLICY"), true
		case "policy":
			return getREST(metod, "BUCKETPOLICY"), true
		default:
			return getREST(metod, "BUCKET"), false
		}
	}
}

func getOperation(object string , r *http.Request) string {
	queries := r.URL.Query()
	var operation string
	var queryFound bool
	for query, _ := range queries {
		if operation, queryFound = getResourceType(object, query, r.Method); queryFound {
			return operation
		}
	}
	return operation
}

func GetAccessLog (r *http.Request, s3errCode s3err.ErrorCode) AccessLog {
	bucket, key := xhttp.GetBucketAndObject(r)
	var errorCode string
	if s3errCode != s3err.ErrNone {
		errorCode = s3err.GetAPIError(s3errCode).Code
	}
	return AccessLog{
		HostHeader: r.Header.Get("Host"),
		RequestID: r.Header.Get("X-Request-ID"),
		RemoteIP: r.Header.Get("X-Real-IP"),
		Requester:  r.Header.Get(xhttp.AmzIdentityId),
		HostId: hostname,
		Bucket: bucket,
		Time: time.Now(),
		Key:  key,
		Operation: getOperation(key, r),
		ErrorCode: errorCode,
	}
}

func Post(r *http.Request, errorCode s3err.ErrorCode) {
	if logger == nil {
		return
	}
	err := logger.Post(tag, GetAccessLog(r, errorCode))
	if err != nil {
		glog.Error("Error while posting log: ", err)
	}
}