package command

import (
	"io/ioutil"
	"path/filepath"
)

func init() {
	cmdScaffold.Run = runScaffold // break init cycle
}

var cmdScaffold = &Command{
	UsageLine: "scaffold [filer]",
	Short:     "generate basic configuration files",
	Long: `Generate filer.toml with all possible configurations for you to customize.

  `,
}

var (
	outputPath = cmdScaffold.Flag.String("output", "", "if not empty, save the configuration file to this directory")
	config     = cmdScaffold.Flag.String("config", "filer", "[filer|replication] the configuration file to generate")
)

func runScaffold(cmd *Command, args []string) bool {

	content := ""
	switch *config {
	case "filer":
		content = FILER_TOML_EXAMPLE
	case "replication":
		content = REPLICATION_TOML_EXAMPLE
	}
	if content == "" {
		println("need a valid -config option")
		return false
	}

	if *outputPath != "" {
		ioutil.WriteFile(filepath.Join(*outputPath, *config+".toml"), []byte(content), 0x755)
	} else {
		println(content)
	}
	return true
}

const (
	FILER_TOML_EXAMPLE = `
# A sample TOML config file for SeaweedFS filer store
# Used with "weed filer" or "weed server -filer"
# Put this file to one of the location, with descending priority
#    ./filer.toml
#    $HOME/.seaweedfs/filer.toml
#    /etc/seaweedfs/filer.toml

[memory]
# local in memory, mostly for testing purpose
enabled = false

[leveldb]
# local on disk, mostly for simple single-machine setup, fairly scalable
enabled = true
dir = "."					# directory to store level db files

####################################################
# multiple filers on shared storage, fairly scalable
####################################################

[mysql]
# CREATE TABLE IF NOT EXISTS filemeta (
#   dirhash     BIGINT        COMMENT 'first 64 bits of MD5 hash value of directory field',
#   name        VARCHAR(1000) COMMENT 'directory or file name',
#   directory   VARCHAR(4096) COMMENT 'full path to parent directory',
#   meta        BLOB,
#   PRIMARY KEY (dirhash, name)
# ) DEFAULT CHARSET=utf8;
enabled = false
hostname = "localhost"
port = 3306
username = "root"
password = ""
database = ""              # create or use an existing database
connection_max_idle = 2
connection_max_open = 100

[postgres]
# CREATE TABLE IF NOT EXISTS filemeta (
#   dirhash     BIGINT,
#   name        VARCHAR(1000),
#   directory   VARCHAR(4096),
#   meta        bytea,
#   PRIMARY KEY (dirhash, name)
# );
enabled = false
hostname = "localhost"
port = 5432
username = "postgres"
password = ""
database = ""              # create or use an existing database
sslmode = "disable"
connection_max_idle = 100
connection_max_open = 100

[cassandra]
# CREATE TABLE filemeta (
#    directory varchar,
#    name varchar,
#    meta blob,
#    PRIMARY KEY (directory, name)
# ) WITH CLUSTERING ORDER BY (name ASC);
enabled = false
keyspace="seaweedfs"
hosts=[
	"localhost:9042",
]

[redis]
enabled = false
address  = "localhost:6379"
password = ""
db = 0

[redis_cluster]
enabled = false
addresses = [
    "localhost:30001",
    "localhost:30002",
    "localhost:30003",
    "localhost:30004",
    "localhost:30005",
    "localhost:30006",
]


####################################################
# notification
# sends filer updates for each file to an external message queue
####################################################
[notification.log]
enabled = false

[notification.kafka]
enabled = false
hosts = [
  "localhost:9092"
]
topic = "seaweedfs_filer"

`
	REPLICATION_TOML_EXAMPLE = `
# A sample TOML config file for replicating SeaweedFS filer
# Used with "weed filer.replicate"
# Put this file to one of the location, with descending priority
#    ./replication.toml
#    $HOME/.seaweedfs/replication.toml
#    /etc/seaweedfs/replication.toml

[source.filer]
enabled = true
grpcAddress = "localhost:18888"
directory = "/buckets"    # all files under this directory tree are replicated

[notification.kafka]
enabled = false
hosts = [
  "localhost:9092"
]
topic = "seaweedfs_filer1_to_filer2"
offsetFile = "./last.offset"
offsetSaveIntervalSeconds = 10

[sink.filer]
enabled = false
grpcAddress = "localhost:18888"
directory = "/backup"    # all replicated files are under this directory tree
replication = ""
collection = ""
ttlSec = 0

[sink.s3]
# experimental
# See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sessions.html
# default loads credentials from the shared credentials file (~/.aws/credentials). 
enabled = false
aws_access_key_id     = ""     # if empty, loads from the shared credentials file (~/.aws/credentials).
aws_secret_access_key = ""     # if empty, loads from the shared credentials file (~/.aws/credentials).
region = "us-east-2"
bucket = "your_bucket_name"    # an existing bucket
directory = ""                 # destination directory (do not prefix or suffix with "/")

[sink.google_cloud_storage]
# experimental
# see https://cloud.google.com/docs/authentication/getting-started
enabled = false
google_application_credentials = "/path/to/x.json" # path to json credential file
projectId = "your_project_id"
bucket = "your_bucket_name"    # an existing bucket
directory = "/"                 # destination directory

`
)
