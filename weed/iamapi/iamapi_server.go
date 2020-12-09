package iamapi

// https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html

import (
	"bytes"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"net/http"
	"strings"
)

type IamServerOption struct {
	Masters          string
	Filer            string
	Port             int
	FilerGrpcAddress string
	GrpcDialOption   grpc.DialOption
}

type IamApiServer struct {
	option       *IamServerOption
	masterClient *wdclient.MasterClient
	filerclient  *filer_pb.SeaweedFilerClient
}

func NewIamApiServer(router *mux.Router, option *IamServerOption) (iamApiServer *IamApiServer, err error) {
	iamApiServer = &IamApiServer{
		option:       option,
		masterClient: wdclient.NewMasterClient(option.GrpcDialOption, pb.AdminShellClient, "", 0, "", strings.Split(option.Masters, ",")),
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

func (iama *IamApiServer) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	var buf bytes.Buffer
	err = pb.WithGrpcFilerClient(iama.option.FilerGrpcAddress, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err = filer.ReadEntry(iama.masterClient, client, filer.IamConfigDirecotry, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if buf.Len() > 0 {
		if err = filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg); err != nil {
			return err
		}
	}
	return nil
}
