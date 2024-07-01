package iamapi

// https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

type IamS3ApiConfig interface {
	GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	GetPolicies(policies *Policies) (err error)
	PutPolicies(policies *Policies) (err error)
}

type IamS3ApiConfigure struct {
	option       *IamServerOption
	masterClient *wdclient.MasterClient
}

type IamServerOption struct {
	Masters        map[string]pb.ServerAddress
	Filer          pb.ServerAddress
	Port           int
	GrpcDialOption grpc.DialOption
}

type IamApiServer struct {
	s3ApiConfig IamS3ApiConfig
	iam         *s3api.IdentityAccessManagement
}

var s3ApiConfigure IamS3ApiConfig

func NewIamApiServer(router *mux.Router, option *IamServerOption) (iamApiServer *IamApiServer, err error) {
	s3ApiConfigure = IamS3ApiConfigure{
		option:       option,
		masterClient: wdclient.NewMasterClient(option.GrpcDialOption, "", "iam", "", "", "", *pb.NewServiceDiscoveryFromMap(option.Masters)),
	}
	s3Option := s3api.S3ApiServerOption{
		Filer:          option.Filer,
		GrpcDialOption: option.GrpcDialOption,
	}
	iamApiServer = &IamApiServer{
		s3ApiConfig: s3ApiConfigure,
		iam:         s3api.NewIdentityAccessManagement(&s3Option),
	}

	iamApiServer.registerRouter(router)

	return iamApiServer, nil
}

func (iama *IamApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	// ListBuckets

	// apiRouter.Methods("GET").Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_ADMIN), "LIST"))
	apiRouter.Methods(http.MethodPost).Path("/").HandlerFunc(iama.iam.Auth(iama.DoActions, ACTION_ADMIN))
	//
	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(s3err.NotFoundHandler)
}

func (iam IamS3ApiConfigure) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	var buf bytes.Buffer
	err = pb.WithGrpcFilerClient(false, 0, iam.option.Filer, iam.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err = filer.ReadEntry(iam.masterClient, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
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

func (iam IamS3ApiConfigure) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	buf := bytes.Buffer{}
	if err := filer.ProtoToText(&buf, s3cfg); err != nil {
		return fmt.Errorf("ProtoToText: %s", err)
	}
	return pb.WithGrpcFilerClient(false, 0, iam.option.Filer, iam.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		err = util.Retry("saveIamIdentity", func() error {
			return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func (iam IamS3ApiConfigure) GetPolicies(policies *Policies) (err error) {
	var buf bytes.Buffer
	err = pb.WithGrpcFilerClient(false, 0, iam.option.Filer, iam.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err = filer.ReadEntry(iam.masterClient, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf); err != nil {
			return err
		}
		return nil
	})
	if err != nil && err != filer_pb.ErrNotFound {
		return err
	}
	if err == filer_pb.ErrNotFound || buf.Len() == 0 {
		policies.Policies = make(map[string]PolicyDocument)
		return nil
	}
	if err := json.Unmarshal(buf.Bytes(), policies); err != nil {
		return err
	}
	return nil
}

func (iam IamS3ApiConfigure) PutPolicies(policies *Policies) (err error) {
	var b []byte
	if b, err = json.Marshal(policies); err != nil {
		return err
	}
	return pb.WithGrpcFilerClient(false, 0, iam.option.Filer, iam.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err := filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamPoliciesFile, b); err != nil {
			return err
		}
		return nil
	})
}
