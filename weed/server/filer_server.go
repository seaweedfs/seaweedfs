package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/arangodb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/elastic/v7"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/etcd"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/hbase"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mongodb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/sqlite"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/ydb"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/aws_sqs"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/google_pub_sub"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/kafka"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/log"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

type FilerOption struct {
	Masters               *pb.ServerDiscovery
	FilerGroup            string
	Collection            string
	DefaultReplication    string
	DisableDirListing     bool
	MaxMB                 int
	DirListingLimit       int
	DataCenter            string
	Rack                  string
	DataNode              string
	DefaultLevelDbDir     string
	DisableHttp           bool
	Host                  pb.ServerAddress
	recursiveDelete       bool
	Cipher                bool
	SaveToFilerLimit      int64
	ConcurrentUploadLimit int64
	ShowUIDirectoryDelete bool
	DownloadMaxBytesPs    int64
	DiskType              string
	AllowedOrigins        []string
	ExposeDirectoryData   bool
}

type FilerServer struct {
	inFlightDataSize int64
	listenersWaits   int64

	// notifying clients
	listenersLock sync.Mutex
	listenersCond *sync.Cond

	inFlightDataLimitCond *sync.Cond

	filer_pb.UnimplementedSeaweedFilerServer
	option         *FilerOption
	secret         security.SigningKey
	filer          *filer.Filer
	filerGuard     *security.Guard
	volumeGuard    *security.Guard
	grpcDialOption grpc.DialOption

	// metrics read from the master
	metricsAddress     string
	metricsIntervalSec int

	// track known metadata listeners
	knownListenersLock sync.Mutex
	knownListeners     map[int32]int32
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	v := util.GetViper()
	signingKey := v.GetString("jwt.filer_signing.key")
	v.SetDefault("jwt.filer_signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.filer_signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.filer_signing.read.key")
	v.SetDefault("jwt.filer_signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.filer_signing.read.expires_after_seconds")

	volumeSigningKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	volumeExpiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	volumeReadSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	volumeReadExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	v.SetDefault("cors.allowed_origins.values", "*")

	allowedOrigins := v.GetString("cors.allowed_origins.values")
	domains := strings.Split(allowedOrigins, ",")
	option.AllowedOrigins = domains

	v.SetDefault("filer.expose_directory_metadata.enabled", true)
	returnDirMetadata := v.GetBool("filer.expose_directory_metadata.enabled")
	option.ExposeDirectoryData = returnDirMetadata

	fs = &FilerServer{
		option:                option,
		grpcDialOption:        security.LoadClientTLS(util.GetViper(), "grpc.filer"),
		knownListeners:        make(map[int32]int32),
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
	}
	fs.listenersCond = sync.NewCond(&fs.listenersLock)

	option.Masters.RefreshBySrvIfAvailable()
	if len(option.Masters.GetInstances()) == 0 {
		glog.Fatal("master list is required!")
	}
	v.SetDefault("filer.options.max_file_name_length", 255)
	maxFilenameLength := v.GetUint32("filer.options.max_file_name_length")
	fs.filer = filer.NewFiler(*option.Masters, fs.grpcDialOption, option.Host, option.FilerGroup, option.Collection, option.DefaultReplication, option.DataCenter, maxFilenameLength, func() {
		if atomic.LoadInt64(&fs.listenersWaits) > 0 {
			fs.listenersCond.Broadcast()
		}
	})
	fs.filer.Cipher = option.Cipher
	whiteList := util.StringSplit(v.GetString("guard.white_list"), ",")
	fs.filerGuard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)
	fs.volumeGuard = security.NewGuard([]string{}, volumeSigningKey, volumeExpiresAfterSec, volumeReadSigningKey, volumeReadExpiresAfterSec)

	fs.checkWithMaster()

	go stats.LoopPushingMetric("filer", string(fs.option.Host), fs.metricsAddress, fs.metricsIntervalSec)
	go fs.filer.KeepMasterClientConnected(context.Background())

	if !util.LoadConfiguration("filer", false) {
		v.SetDefault("leveldb2.enabled", true)
		v.SetDefault("leveldb2.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
		glog.V(0).Infof("default to create filer store dir in %s", option.DefaultLevelDbDir)
	} else {
		glog.Warningf("skipping default store dir in %s", option.DefaultLevelDbDir)
	}
	util.LoadConfiguration("notification", false)

	fs.option.recursiveDelete = v.GetBool("filer.options.recursive_delete")
	v.SetDefault("filer.options.buckets_folder", "/buckets")
	fs.filer.DirBucketsPath = v.GetString("filer.options.buckets_folder")
	// TODO deprecated, will be removed after 2020-12-31
	// replaced by https://github.com/seaweedfs/seaweedfs/wiki/Path-Specific-Configuration
	// fs.filer.FsyncBuckets = v.GetStringSlice("filer.options.buckets_fsync")
	isFresh := fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v, "notification.")

	handleStaticResources(defaultMux)
	if !option.DisableHttp {
		defaultMux.HandleFunc("/healthz", fs.filerHealthzHandler)
		defaultMux.HandleFunc("/", fs.filerGuard.WhiteList(fs.filerHandler))
	}
	if defaultMux != readonlyMux {
		handleStaticResources(readonlyMux)
		readonlyMux.HandleFunc("/healthz", fs.filerHealthzHandler)
		readonlyMux.HandleFunc("/", fs.filerGuard.WhiteList(fs.readonlyFilerHandler))
	}

	existingNodes := fs.filer.ListExistingPeerUpdates(context.Background())
	startFromTime := time.Now().Add(-filer.LogFlushInterval)
	if isFresh {
		glog.V(0).Infof("%s bootstrap from peers %+v", option.Host, existingNodes)
		if err := fs.filer.MaybeBootstrapFromOnePeer(option.Host, existingNodes, startFromTime); err != nil {
			glog.Fatalf("%s bootstrap from %+v: %v", option.Host, existingNodes, err)
		}
	}
	fs.filer.AggregateFromPeers(option.Host, existingNodes, startFromTime)

	fs.filer.LoadFilerConf()

	fs.filer.LoadRemoteStorageConfAndMapping()

	grace.OnReload(fs.Reload)
	grace.OnInterrupt(func() {
		fs.filer.Shutdown()
	})

	fs.filer.Dlm.LockRing.SetTakeSnapshotCallback(fs.OnDlmChangeSnapshot)

	return fs, nil
}

func (fs *FilerServer) checkWithMaster() {

	isConnected := false
	for !isConnected {
		fs.option.Masters.RefreshBySrvIfAvailable()
		for _, master := range fs.option.Masters.GetInstances() {
			readErr := operation.WithMasterServerClient(false, master, fs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
				resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return fmt.Errorf("get master %s configuration: %v", master, err)
				}
				fs.metricsAddress, fs.metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)
				return nil
			})
			if readErr == nil {
				isConnected = true
			} else {
				time.Sleep(7 * time.Second)
			}
		}
	}
}

func (fs *FilerServer) Reload() {
	glog.V(0).Infoln("Reload filer server...")

	util.LoadConfiguration("security", false)
	v := util.GetViper()
	fs.filerGuard.UpdateWhiteList(util.StringSplit(v.GetString("guard.white_list"), ","))
}
