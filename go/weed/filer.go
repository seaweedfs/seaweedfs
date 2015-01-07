package main

import (
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/util"
	"github.com/chrislusf/weed-fs/go/weed/weed_server"
)

var (
	f FilerOptions
)

type FilerOptions struct {
	master                  *string
	port                    *int
	collection              *string
	defaultReplicaPlacement *string
	dir                     *string
	redirectOnRead          *bool
	cassandra_server        *string
	cassandra_keyspace      *string
}

func init() {
	cmdFiler.Run = runFiler // break init cycle
	f.master = cmdFiler.Flag.String("master", "localhost:9333", "master server location")
	f.collection = cmdFiler.Flag.String("collection", "", "all data will be stored in this collection")
	f.port = cmdFiler.Flag.Int("port", 8888, "filer server http listen port")
	f.dir = cmdFiler.Flag.String("dir", os.TempDir(), "directory to store meta data")
	f.defaultReplicaPlacement = cmdFiler.Flag.String("defaultReplicaPlacement", "000", "default replication type if not specified")
	f.redirectOnRead = cmdFiler.Flag.Bool("redirectOnRead", false, "whether proxy or redirect to volume server during file GET request")
	f.cassandra_server = cmdFiler.Flag.String("cassandra.server", "", "host[:port] of the cassandra server")
	f.cassandra_keyspace = cmdFiler.Flag.String("cassandra.keyspace", "seaweed", "keyspace of the cassandra server")
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

	r := http.NewServeMux()
	_, nfs_err := weed_server.NewFilerServer(r, *f.port, *f.master, *f.dir, *f.collection,
		*f.defaultReplicaPlacement, *f.redirectOnRead,
		*f.cassandra_server, *f.cassandra_keyspace,
	)
	if nfs_err != nil {
		glog.Fatalf(nfs_err.Error())
	}
	glog.V(0).Infoln("Start Seaweed Filer", util.VERSION, "at port", strconv.Itoa(*f.port))
	filerListener, e := util.NewListener(
		":"+strconv.Itoa(*f.port),
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf(e.Error())
	}
	if e := http.Serve(filerListener, r); e != nil {
		glog.Fatalf("Filer Fail to serve:%s", e.Error())
	}

	return true
}
