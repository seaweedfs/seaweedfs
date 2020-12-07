package s3api

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	. "github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
)

type S3ApiServerOption struct {
	Filer            string
	Port             int
	FilerGrpcAddress string
	Config           string
	DomainName       string
	BucketsPath      string
	GrpcDialOption   grpc.DialOption
}

type S3ApiServer struct {
	option *S3ApiServerOption
	iam    *IdentityAccessManagement
}

func NewS3ApiServer(router *mux.Router, option *S3ApiServerOption) (s3ApiServer *S3ApiServer, err error) {
	s3ApiServer = &S3ApiServer{
		option: option,
		iam:    NewIdentityAccessManagement(option),
	}

	s3ApiServer.registerRouter(router)

	go s3ApiServer.subscribeMetaEvents("s3", filer.IamConfigDirecotry+"/"+filer.IamIdentityFile, time.Now().UnixNano())

	return s3ApiServer, nil
}

func (s3a *S3ApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	var routers []*mux.Router
	if s3a.option.DomainName != "" {
		domainNames := strings.Split(s3a.option.DomainName, ",")
		for _, domainName := range domainNames {
			routers = append(routers, apiRouter.Host(
				fmt.Sprintf("%s.%s:%d", "{bucket:.+}", domainName, s3a.option.Port)).Subrouter())
			routers = append(routers, apiRouter.Host(
				fmt.Sprintf("%s.%s", "{bucket:.+}", domainName)).Subrouter())
		}
	}
	routers = append(routers, apiRouter.PathPrefix("/{bucket}").Subrouter())

	for _, bucket := range routers {

		// HeadObject
		bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.HeadObjectHandler, ACTION_READ), "GET"))
		// HeadBucket
		bucket.Methods("HEAD").HandlerFunc(track(s3a.iam.Auth(s3a.HeadBucketHandler, ACTION_ADMIN), "GET"))

		// CopyObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(track(s3a.iam.Auth(s3a.CopyObjectPartHandler, ACTION_WRITE), "PUT")).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.PutObjectPartHandler, ACTION_WRITE), "PUT")).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.CompleteMultipartUploadHandler, ACTION_WRITE), "POST")).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.NewMultipartUploadHandler, ACTION_WRITE), "POST")).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.AbortMultipartUploadHandler, ACTION_WRITE), "DELETE")).Queries("uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.ListObjectPartsHandler, ACTION_READ), "GET")).Queries("uploadId", "{uploadId:.*}")
		// ListMultipartUploads
		bucket.Methods("GET").HandlerFunc(track(s3a.iam.Auth(s3a.ListMultipartUploadsHandler, ACTION_READ), "GET")).Queries("uploads", "")

		// GetObjectTagging
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.GetObjectTaggingHandler, ACTION_READ), "GET")).Queries("tagging", "")
		// PutObjectTagging
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.PutObjectTaggingHandler, ACTION_TAGGING), "PUT")).Queries("tagging", "")
		// DeleteObjectTagging
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.DeleteObjectTaggingHandler, ACTION_TAGGING), "DELETE")).Queries("tagging", "")

		// CopyObject
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(track(s3a.iam.Auth(s3a.CopyObjectHandler, ACTION_WRITE), "COPY"))
		// PutObject
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.PutObjectHandler, ACTION_WRITE), "PUT"))
		// PutBucket
		bucket.Methods("PUT").HandlerFunc(track(s3a.iam.Auth(s3a.PutBucketHandler, ACTION_ADMIN), "PUT"))

		// DeleteObject
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.DeleteObjectHandler, ACTION_WRITE), "DELETE"))
		// DeleteBucket
		bucket.Methods("DELETE").HandlerFunc(track(s3a.iam.Auth(s3a.DeleteBucketHandler, ACTION_WRITE), "DELETE"))

		// ListObjectsV2
		bucket.Methods("GET").HandlerFunc(track(s3a.iam.Auth(s3a.ListObjectsV2Handler, ACTION_LIST), "LIST")).Queries("list-type", "2")
		// GetObject, but directory listing is not supported
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(track(s3a.iam.Auth(s3a.GetObjectHandler, ACTION_READ), "GET"))
		// ListObjectsV1 (Legacy)
		bucket.Methods("GET").HandlerFunc(track(s3a.iam.Auth(s3a.ListObjectsV1Handler, ACTION_LIST), "LIST"))

		// PostPolicy
		bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(track(s3a.iam.Auth(s3a.PostPolicyBucketHandler, ACTION_WRITE), "POST"))

		// DeleteMultipleObjects
		bucket.Methods("POST").HandlerFunc(track(s3a.iam.Auth(s3a.DeleteMultipleObjectsHandler, ACTION_WRITE), "DELETE")).Queries("delete", "")
		/*

			// not implemented
			// GetBucketLocation
			bucket.Methods("GET").HandlerFunc(s3a.GetBucketLocationHandler).Queries("location", "")
			// GetBucketPolicy
			bucket.Methods("GET").HandlerFunc(s3a.GetBucketPolicyHandler).Queries("policy", "")
			// GetObjectACL
			bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(s3a.GetObjectACLHandler).Queries("acl", "")
			// GetBucketACL
			bucket.Methods("GET").HandlerFunc(s3a.GetBucketACLHandler).Queries("acl", "")
			// PutBucketPolicy
			bucket.Methods("PUT").HandlerFunc(s3a.PutBucketPolicyHandler).Queries("policy", "")
			// DeleteBucketPolicy
			bucket.Methods("DELETE").HandlerFunc(s3a.DeleteBucketPolicyHandler).Queries("policy", "")
		*/

	}

	// ListBuckets
	apiRouter.Methods("GET").Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_ADMIN), "LIST"))

	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(notFoundHandler)

}
