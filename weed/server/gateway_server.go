package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/operation"
	"google.golang.org/grpc"
	"math/rand"
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/util"

	_ "github.com/chrislusf/seaweedfs/weed/filer/cassandra"
	_ "github.com/chrislusf/seaweedfs/weed/filer/elastic/v7"
	_ "github.com/chrislusf/seaweedfs/weed/filer/etcd"
	_ "github.com/chrislusf/seaweedfs/weed/filer/hbase"
	_ "github.com/chrislusf/seaweedfs/weed/filer/leveldb"
	_ "github.com/chrislusf/seaweedfs/weed/filer/leveldb2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/leveldb3"
	_ "github.com/chrislusf/seaweedfs/weed/filer/mongodb"
	_ "github.com/chrislusf/seaweedfs/weed/filer/mysql"
	_ "github.com/chrislusf/seaweedfs/weed/filer/mysql2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/postgres"
	_ "github.com/chrislusf/seaweedfs/weed/filer/postgres2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/redis"
	_ "github.com/chrislusf/seaweedfs/weed/filer/redis2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	_ "github.com/chrislusf/seaweedfs/weed/notification/aws_sqs"
	_ "github.com/chrislusf/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "github.com/chrislusf/seaweedfs/weed/notification/google_pub_sub"
	_ "github.com/chrislusf/seaweedfs/weed/notification/kafka"
	_ "github.com/chrislusf/seaweedfs/weed/notification/log"
	"github.com/chrislusf/seaweedfs/weed/security"
)

type GatewayOption struct {
	Masters  []string
	Filers   []string
	MaxMB    int
	IsSecure bool
}

type GatewayServer struct {
	option         *GatewayOption
	secret         security.SigningKey
	grpcDialOption grpc.DialOption
}

func NewGatewayServer(defaultMux *http.ServeMux, option *GatewayOption) (fs *GatewayServer, err error) {

	fs = &GatewayServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.client"),
	}

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	defaultMux.HandleFunc("/blobs/", fs.blobsHandler)
	defaultMux.HandleFunc("/files/", fs.filesHandler)
	defaultMux.HandleFunc("/topics/", fs.topicsHandler)

	return fs, nil
}

func (fs *GatewayServer) getMaster() string {
	randMaster := rand.Intn(len(fs.option.Masters))
	return fs.option.Masters[randMaster]
}

func (fs *GatewayServer) blobsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "DELETE":
		chunkId := r.URL.Path[len("/blobs/"):]
		fullUrl, err := operation.LookupFileId(fs.getMaster, chunkId)
		if err != nil {
			writeJsonError(w, r, http.StatusNotFound, err)
			return
		}
		var jwtAuthorization security.EncodedJwt
		if fs.option.IsSecure {
			jwtAuthorization = operation.LookupJwt(fs.getMaster(), chunkId)
		}
		body, statusCode, err := util.DeleteProxied(fullUrl, string(jwtAuthorization))
		if err != nil {
			writeJsonError(w, r, http.StatusNotFound, err)
			return
		}
		w.WriteHeader(statusCode)
		w.Write(body)
	case "POST":
		submitForClientHandler(w, r, fs.getMaster, fs.grpcDialOption)
	}
}

func (fs *GatewayServer) filesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "DELETE":
	case "POST":
	}
}

func (fs *GatewayServer) topicsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
	}
}
