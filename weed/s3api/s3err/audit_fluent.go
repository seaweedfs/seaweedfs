package s3err

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

type AccessLogExtend struct {
	AccessLog
	AccessLogHTTP
}

type AccessLog struct {
	Bucket           string `msg:"bucket" json:"bucket"`                   // awsexamplebucket1
	Time             int64  `msg:"time" json:"time"`                       // [06/Feb/2019:00:00:38 +0000]
	RemoteIP         string `msg:"remote_ip" json:"remote_ip,omitempty"`   // 192.0.2.3
	Requester        string `msg:"requester" json:"requester,omitempty"`   // IAM user id
	RequestID        string `msg:"request_id" json:"request_id,omitempty"` // 3E57427F33A59F07
	Operation        string `msg:"operation" json:"operation,omitempty"`   // REST.HTTP_method.resource_type REST.PUT.OBJECT
	Key              string `msg:"key" json:"key,omitempty"`               // /photos/2019/08/puppy.jpg
	ErrorCode        string `msg:"error_code" json:"error_code,omitempty"`
	HostId           string `msg:"host_id" json:"host_id,omitempty"`
	HostHeader       string `msg:"host_header" json:"host_header,omitempty"` // s3.us-west-2.amazonaws.com
	UserAgent        string `msg:"user_agent" json:"user_agent,omitempty"`
	HTTPStatus       int    `msg:"status" json:"status,omitempty"`
	SignatureVersion string `msg:"signature_version" json:"signature_version,omitempty"`
}

type AccessLogHTTP struct {
	RequestURI         string `json:"request_uri,omitempty"` // "GET /awsexamplebucket1/photos/2019/08/puppy.jpg?x-foo=bar HTTP/1.1"
	BytesSent          string `json:"bytes_sent,omitempty"`
	ObjectSize         string `json:"object_size,omitempty"`
	TotalTime          int    `json:"total_time,omitempty"`
	TurnAroundTime     int    `json:"turn_around_time,omitempty"`
	Referer            string `json:"Referer,omitempty"`
	VersionId          string `json:"version_id,omitempty"`
	CipherSuite        string `json:"cipher_suite,omitempty"`
	AuthenticationType string `json:"auth_type,omitempty"`
	TLSVersion         string `json:"TLS_version,omitempty"`
}

const tag = "s3.access"

var (
	Logger      *fluent.Fluent
	hostname    = os.Getenv("HOSTNAME")
	environment = os.Getenv("ENVIRONMENT")
)

func InitAuditLog(config string) {
	configContent, readErr := os.ReadFile(config)
	if readErr != nil {
		glog.Errorf("fail to read fluent config %s : %v", config, readErr)
		return
	}
	fluentConfig := &fluent.Config{}
	if err := json.Unmarshal(configContent, fluentConfig); err != nil {
		glog.Errorf("fail to parse fluent config %s : %v", string(configContent), err)
		return
	}
	if len(fluentConfig.TagPrefix) == 0 && len(environment) > 0 {
		fluentConfig.TagPrefix = environment
	}
	fluentConfig.Async = true
	fluentConfig.AsyncResultCallback = func(data []byte, err error) {
		if err != nil {
			glog.Warning("Error while posting log: ", err)
		}
	}
	var err error
	Logger, err = fluent.New(*fluentConfig)
	if err != nil {
		glog.Errorf("fail to load fluent config: %v", err)
	}
}

func getREST(httpMetod string, resourceType string) string {
	return fmt.Sprintf("REST.%s.%s", httpMetod, resourceType)
}

func getResourceType(object string, query_key string, metod string) (string, bool) {
	if object == "/" {
		switch query_key {
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
	} else {
		switch query_key {
		case "tagging":
			return getREST(metod, "OBJECTTAGGING"), true
		default:
			return getREST(metod, "OBJECT"), false
		}
	}
}

func getOperation(object string, r *http.Request) string {
	queries := r.URL.Query()
	var operation string
	var queryFound bool
	for key, _ := range queries {
		operation, queryFound = getResourceType(object, key, r.Method)
		if queryFound {
			return operation
		}
	}
	if len(queries) == 0 {
		operation, _ = getResourceType(object, "", r.Method)
	}
	return operation
}

func GetAccessHttpLog(r *http.Request, statusCode int, s3errCode ErrorCode) AccessLogHTTP {
	return AccessLogHTTP{
		RequestURI: r.RequestURI,
		Referer:    r.Header.Get("Referer"),
	}
}

func GetAccessLog(r *http.Request, HTTPStatusCode int, s3errCode ErrorCode) *AccessLog {
	bucket, key := s3_constants.GetBucketAndObject(r)
	var errorCode string
	if s3errCode != ErrNone {
		errorCode = GetAPIError(s3errCode).Code
	}
	remoteIP := r.Header.Get("X-Real-IP")
	if len(remoteIP) == 0 {
		remoteIP = r.RemoteAddr
	}
	hostHeader := r.Header.Get("X-Forwarded-Host")
	if len(hostHeader) == 0 {
		hostHeader = r.Host
	}
	return &AccessLog{
		HostHeader:       hostHeader,
		RequestID:        r.Header.Get("X-Request-ID"),
		RemoteIP:         remoteIP,
		Requester:        r.Header.Get(s3_constants.AmzIdentityId),
		SignatureVersion: r.Header.Get(s3_constants.AmzAuthType),
		UserAgent:        r.Header.Get("user-agent"),
		HostId:           hostname,
		Bucket:           bucket,
		HTTPStatus:       HTTPStatusCode,
		Time:             time.Now().Unix(),
		Key:              key,
		Operation:        getOperation(key, r),
		ErrorCode:        errorCode,
	}
}

func PostLog(r *http.Request, HTTPStatusCode int, errorCode ErrorCode) {
	if Logger == nil {
		return
	}
	if err := Logger.Post(tag, *GetAccessLog(r, HTTPStatusCode, errorCode)); err != nil {
		glog.Warning("Error while posting log: ", err)
	}
}

func PostAccessLog(log AccessLog) {
	if Logger == nil || len(log.Key) == 0 {
		return
	}
	if err := Logger.Post(tag, log); err != nil {
		glog.Warning("Error while posting log: ", err)
	}
}
