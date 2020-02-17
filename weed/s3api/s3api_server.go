package s3api

import (
	"net/http"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
)

type S3ApiServerOption struct {
	Filer            string
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
		iam:    NewIdentityAccessManagement(option.Config, option.DomainName),
	}

	s3ApiServer.registerRouter(router)

	return s3ApiServer, nil
}

func (s3a *S3ApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	var routers []*mux.Router
	if s3a.option.DomainName != "" {
		routers = append(routers, apiRouter.Host("{bucket:.+}."+s3a.option.DomainName).Subrouter())
	}
	routers = append(routers, apiRouter.PathPrefix("/{bucket}").Subrouter())

	for _, bucket := range routers {

		// HeadObject
		bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.HeadObjectHandler, ACTION_READ))
		// HeadBucket
		bucket.Methods("HEAD").HandlerFunc(s3a.iam.Auth(s3a.HeadBucketHandler, ACTION_ADMIN))

		// CopyObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(s3a.iam.Auth(s3a.CopyObjectPartHandler, ACTION_WRITE)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.PutObjectPartHandler, ACTION_WRITE)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.CompleteMultipartUploadHandler, ACTION_WRITE)).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.NewMultipartUploadHandler, ACTION_WRITE)).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.AbortMultipartUploadHandler, ACTION_WRITE)).Queries("uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.ListObjectPartsHandler, ACTION_WRITE)).Queries("uploadId", "{uploadId:.*}")
		// ListMultipartUploads
		bucket.Methods("GET").HandlerFunc(s3a.iam.Auth(s3a.ListMultipartUploadsHandler, ACTION_WRITE)).Queries("uploads", "")

		// CopyObject
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(s3a.iam.Auth(s3a.CopyObjectHandler, ACTION_WRITE))
		// PutObject
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.PutObjectHandler, ACTION_WRITE))
		// PutBucket
		bucket.Methods("PUT").HandlerFunc(s3a.iam.Auth(s3a.PutBucketHandler, ACTION_ADMIN))

		// DeleteObject
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.DeleteObjectHandler, ACTION_WRITE))
		// DeleteBucket
		bucket.Methods("DELETE").HandlerFunc(s3a.iam.Auth(s3a.DeleteBucketHandler, ACTION_WRITE))

		// ListObjectsV2
		bucket.Methods("GET").HandlerFunc(s3a.iam.Auth(s3a.ListObjectsV2Handler, ACTION_READ)).Queries("list-type", "2")
		// GetObject, but directory listing is not supported
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(s3a.iam.Auth(s3a.GetObjectHandler, ACTION_READ))
		// ListObjectsV1 (Legacy)
		bucket.Methods("GET").HandlerFunc(s3a.iam.Auth(s3a.ListObjectsV1Handler, ACTION_READ))

		// DeleteMultipleObjects
		bucket.Methods("POST").HandlerFunc(s3a.iam.Auth(s3a.DeleteMultipleObjectsHandler, ACTION_WRITE)).Queries("delete", "")
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
			// PostPolicy
			bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(s3a.PostPolicyBucketHandler)
		*/

	}

	// ListBuckets
	apiRouter.Methods("GET").Path("/").HandlerFunc(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_ADMIN))

	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(notFoundHandler)

}
