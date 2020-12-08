package iamapi

// https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html

import (
	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"net/http"
)

type IamServerOption struct {
	Filer            string
	Port             int
	FilerGrpcAddress string
	GrpcDialOption   grpc.DialOption
}

type IamApiServer struct {
	option *IamServerOption
	ifs    *IAMFilerStore
}

func NewIamApiServer(router *mux.Router, option *IamServerOption, ifs *IAMFilerStore) (iamApiServer *IamApiServer, err error) {
	iamApiServer = &IamApiServer{
		option: option,
		ifs:    ifs,
	}

	iamApiServer.registerRouter(router)

	return iamApiServer, nil
}

func (iama *IamApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	// ListBuckets

	// apiRouter.Methods("GET").Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_ADMIN), "LIST"))
	apiRouter.Path("/").Methods("POST").HandlerFunc(iama.DoActions)
	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(notFoundHandler)
}
