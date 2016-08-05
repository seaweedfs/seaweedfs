package command

import (
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strings"
)

var (
	f FilerOptions
)

type FilerOptions struct {
	master                  *string
	ip                      *string
	port                    *int
	collection              *string
	defaultReplicaPlacement *string
	dir                     *string
	redirectOnRead          *bool
	disableDirListing       *bool
	maxMB			*int
	secretKey               *string
	cassandra_server        *string
	cassandra_keyspace      *string
	redis_server            *string
	redis_password          *string
	redis_database          *int
	get_ip_whitelist_option      *string
	get_root_whitelist_option    *string
	head_ip_whitelist_option     *string
	head_root_whitelist_option   *string
	delete_ip_whitelist_option   *string
	delete_root_whitelist_option *string
	put_ip_whitelist_option      *string
	put_root_whitelist_option    *string
	post_ip_whitelist_option     *string
	post_root_whitelist_option   *string
	get_secure_key               *string
	head_secure_key              *string
	delete_secure_key            *string
	put_secure_key               *string
	post_secure_key              *string
	get_ip_whitelist      []string
	get_root_whitelist    []string
	head_ip_whitelist     []string
	head_root_whitelist   []string
	delete_ip_whitelist   []string
	delete_root_whitelist []string
	put_ip_whitelist      []string
	put_root_whitelist    []string
	post_ip_whitelist     []string
	post_root_whitelist   []string
}

func init() {
	cmdFiler.Run = runFiler // break init cycle
	f.master = cmdFiler.Flag.String("master", "localhost:9333", "master server location")
	f.collection = cmdFiler.Flag.String("collection", "", "all data will be stored in this collection")
	f.ip = cmdFiler.Flag.String("ip", "", "filer server http listen ip address")
	f.port = cmdFiler.Flag.Int("port", 8888, "filer server http listen port")
	f.dir = cmdFiler.Flag.String("dir", os.TempDir(), "directory to store meta data")
	f.defaultReplicaPlacement = cmdFiler.Flag.String("defaultReplicaPlacement", "000", "default replication type if not specified")
	f.redirectOnRead = cmdFiler.Flag.Bool("redirectOnRead", false, "whether proxy or redirect to volume server during file GET request")
	f.disableDirListing = cmdFiler.Flag.Bool("disableDirListing", false, "turn off directory listing")
	f.maxMB = cmdFiler.Flag.Int("maxMB", 0, "split files larger than the limit")
	f.cassandra_server = cmdFiler.Flag.String("cassandra.server", "", "host[:port] of the cassandra server")
	f.cassandra_keyspace = cmdFiler.Flag.String("cassandra.keyspace", "seaweed", "keyspace of the cassandra server")
	f.redis_server = cmdFiler.Flag.String("redis.server", "", "host:port of the redis server, e.g., 127.0.0.1:6379")
	f.redis_password = cmdFiler.Flag.String("redis.password", "", "password in clear text")
	f.redis_database = cmdFiler.Flag.Int("redis.database", 0, "the database on the redis server")
	f.secretKey = cmdFiler.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	f.get_ip_whitelist_option = cmdFiler.Flag.String("whitelist.ip.get", "", "comma separated Ip addresses having get permission. No limit if empty.")
	f.get_root_whitelist_option = cmdFiler.Flag.String("whitelist.root.get", "", "comma separated root paths having get permission. No limit if empty.")
	f.head_ip_whitelist_option = cmdFiler.Flag.String("whitelist.ip.head", "", "comma separated Ip addresses having head permission. No limit if empty.")
	f.head_root_whitelist_option = cmdFiler.Flag.String("whitelist.root.head", "", "comma separated root paths having head permission. No limit if empty.")
	f.delete_ip_whitelist_option = cmdFiler.Flag.String("whitelist.ip.delete", "", "comma separated Ip addresses having delete permission. No limit if empty.")
	f.delete_root_whitelist_option = cmdFiler.Flag.String("whitelist.root.delete", "", "comma separated root paths having delete permission. No limit if empty.")
	f.put_ip_whitelist_option = cmdFiler.Flag.String("whitelist.ip.put", "", "comma separated Ip addresses having put permission. No limit if empty.")
	f.put_root_whitelist_option = cmdFiler.Flag.String("whitelist.root.put", "", "comma separated root paths having put permission. No limit if empty.")
	f.post_ip_whitelist_option = cmdFiler.Flag.String("whitelist.ip.post", "", "comma separated Ip addresses having post permission. No limit if empty.")
	f.post_root_whitelist_option = cmdFiler.Flag.String("whitelist.root.post", "", "comma separated root paths having post permission. No limit if empty.")
	f.get_secure_key = cmdFiler.Flag.String("secure.secret.get", "", "secret to encrypt Json Web Token(JWT)")
	f.head_secure_key = cmdFiler.Flag.String("secure.secret.head", "", "secret to encrypt Json Web Token(JWT)")
	f.delete_secure_key = cmdFiler.Flag.String("secure.secret.delete", "", "secret to encrypt Json Web Token(JWT)")
	f.put_secure_key = cmdFiler.Flag.String("secure.secret.put", "", "secret to encrypt Json Web Token(JWT)")
	f.post_secure_key = cmdFiler.Flag.String("secure.secret.post", "", "secret to encrypt Json Web Token(JWT)")

}

var cmdFiler = &Command{
	UsageLine: "filer -port=8888 -dir=/tmp -master=<ip:port>",
	Short:     "start a file server that points to a master server",
	Long: `start a file server which accepts REST operation for any files.

	//create or overwrite the file, the directories /path/to will be automatically created
	POST /path/to/file
	//get the file content
	GET /path/to/file
	//create or overwrite the file, the filename in the multipart request will be used
	POST /path/to/
	//return a json format subdirectory and files listing
	GET /path/to/

  Current <fullpath~fileid> mapping metadata store is local embedded leveldb.
  It should be highly scalable to hundreds of millions of files on a modest machine.

  Future we will ensure it can avoid of being SPOF.

  `,
}

func runFiler(cmd *Command, args []string) bool {

	if err := util.TestFolderWritable(*f.dir); err != nil {
		glog.Fatalf("Check Meta Folder (-dir) Writable %s : %s", *f.dir, err)
	}

	if *f.get_ip_whitelist_option != "" {
		f.get_ip_whitelist = strings.Split(*f.get_ip_whitelist_option, ",")
	}
	if *f.get_root_whitelist_option != "" {
		f.get_root_whitelist = strings.Split(*f.get_root_whitelist_option, ",")
	}
	if *f.head_ip_whitelist_option != "" {
		f.head_ip_whitelist = strings.Split(*f.head_ip_whitelist_option, ",")
	}
	if *f.head_root_whitelist_option != "" {
		f.head_root_whitelist = strings.Split(*f.head_root_whitelist_option, ",")
	}
	if *f.delete_ip_whitelist_option != "" {
		f.delete_ip_whitelist = strings.Split(*f.delete_ip_whitelist_option, ",")
	}
	if *f.delete_root_whitelist_option != "" {
		f.delete_root_whitelist = strings.Split(*f.delete_root_whitelist_option, ",")
	}
	if *f.put_ip_whitelist_option != "" {
		f.put_ip_whitelist = strings.Split(*f.put_ip_whitelist_option, ",")
	}
	if *f.put_root_whitelist_option != "" {
		f.put_root_whitelist = strings.Split(*f.put_root_whitelist_option, ",")
	}
	if *f.post_ip_whitelist_option != "" {
		f.post_ip_whitelist = strings.Split(*f.post_ip_whitelist_option, ",")
	}
	if *f.post_root_whitelist_option != "" {
		f.post_root_whitelist = strings.Split(*f.post_root_whitelist_option, ",")
	}
	r := http.NewServeMux()
	_, nfs_err := weed_server.NewFilerServer(r, *f.ip, *f.port, *f.master, *f.dir, *f.collection,
		*f.defaultReplicaPlacement, *f.redirectOnRead, *f.disableDirListing,
		*f.maxMB,
		*f.secretKey,
		*f.cassandra_server, *f.cassandra_keyspace,
		*f.redis_server, *f.redis_password, *f.redis_database,
		f.get_ip_whitelist, f.head_ip_whitelist, f.delete_ip_whitelist, f.put_ip_whitelist, f.post_ip_whitelist,
		f.get_root_whitelist, f.head_root_whitelist, f.delete_root_whitelist, f.put_root_whitelist, f.post_root_whitelist,
		*f.get_secure_key, *f.head_secure_key, *f.delete_secure_key, *f.put_secure_key, *f.post_secure_key,
	)
	if nfs_err != nil {
		glog.Fatalf("Filer startup error: %v", nfs_err)
	}
	glog.V(0).Infoln("Start Seaweed Filer", util.VERSION, "at port", strconv.Itoa(*f.port))
	filerListener, e := util.NewListener(
		":"+strconv.Itoa(*f.port),
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf("Filer listener error: %v", e)
	}
	if e := http.Serve(filerListener, r); e != nil {
		glog.Fatalf("Filer Fail to serve: %v", e)
	}

	return true
}
