package weed_server

import (
	"net/http"
	"os"

	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/cassandra"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/leveldb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/memdb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/mysql"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/postgres"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/redis"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	_ "github.com/chrislusf/seaweedfs/weed/notification/aws_sqs"
	_ "github.com/chrislusf/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "github.com/chrislusf/seaweedfs/weed/notification/google_pub_sub"
	_ "github.com/chrislusf/seaweedfs/weed/notification/kafka"
	_ "github.com/chrislusf/seaweedfs/weed/notification/log"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"
)

type FilerOption struct {
	Masters            []string
	Collection         string
	DefaultReplication string
	RedirectOnRead     bool
	DisableDirListing  bool
	MaxMB              int
	DirListingLimit    int
	DataCenter         string
	DefaultLevelDbDir  string
	DisableHttp        bool
	MetricsAddress     string
	MetricsIntervalSec int
	Port               int
}

type FilerServer struct {
	option         *FilerOption
	secret         security.SigningKey
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	fs = &FilerServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(viper.Sub("grpc"), "filer"),
	}

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer2.NewFiler(option.Masters, fs.grpcDialOption)

	go fs.filer.KeepConnectedToMaster()

	v := viper.GetViper()
	if !util.LoadConfiguration("filer", false) {
		v.Set("leveldb.enabled", true)
		v.Set("leveldb.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
	}
	util.LoadConfiguration("notification", false)

	fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v.Sub("notification"))

	handleStaticResources(defaultMux)
	if !option.DisableHttp {
		defaultMux.HandleFunc("/", fs.filerHandler)
	}
	if defaultMux != readonlyMux {
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	stats.StartPushingMetric("filer", stats.SourceName(option.Port), stats.FilerGather, option.MetricsAddress, option.MetricsIntervalSec)

	return fs, nil
}
