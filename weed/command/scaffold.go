package command

import (
	"io/ioutil"
	"path/filepath"
)

func init() {
	cmdScaffold.Run = runScaffold // break init cycle
}

var cmdScaffold = &Command{
	UsageLine: "scaffold -config=[filer|notification|replication|security|master]",
	Short:     "generate basic configuration files",
	Long: `Generate filer.toml with all possible configurations for you to customize.

	The options can also be overwritten by environment variables.
	For example, the filer.toml mysql password can be overwritten by environment variable
		export WEED_MYSQL_PASSWORD=some_password
	Environment variable rules:
		* Prefix the variable name with "WEED_"
		* Upppercase the reset of variable name.
		* Replace '.' with '_'

  `,
}

var (
	outputPath = cmdScaffold.Flag.String("output", "", "if not empty, save the configuration file to this directory")
	config     = cmdScaffold.Flag.String("config", "filer", "[filer|notification|replication|security|master] the configuration file to generate")
)

func runScaffold(cmd *Command, args []string) bool {

	content := ""
	switch *config {
	case "filer":
		content = FILER_TOML_EXAMPLE
	case "notification":
		content = NOTIFICATION_TOML_EXAMPLE
	case "replication":
		content = REPLICATION_TOML_EXAMPLE
	case "security":
		content = SECURITY_TOML_EXAMPLE
	case "master":
		content = MASTER_TOML_EXAMPLE
	}
	if content == "" {
		println("need a valid -config option")
		return false
	}

	if *outputPath != "" {
		ioutil.WriteFile(filepath.Join(*outputPath, *config+".toml"), []byte(content), 0644)
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

####################################################
# Customizable filer server options
####################################################
[filer.options]
# with http DELETE, by default the filer would check whether a folder is empty.
# recursive_delete will delete all sub folders and files, similar to "rm -Rf"
recursive_delete = false
# directories under this folder will be automatically creating a separate bucket
buckets_folder = "/buckets"

####################################################
# The following are filer store options
####################################################

[leveldb2]
# local on disk, mostly for simple single-machine setup, fairly scalable
# faster than previous leveldb, recommended.
enabled = true
dir = "."					# directory to store level db files

[mysql]  # or tidb
# CREATE TABLE IF NOT EXISTS filemeta (
#   dirhash     BIGINT         COMMENT 'first 64 bits of MD5 hash value of directory field',
#   name        VARCHAR(1000)  COMMENT 'directory or file name',
#   directory   TEXT           COMMENT 'full path to parent directory',
#   meta        LONGBLOB,
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
interpolateParams = false

[postgres] # or cockroachdb
# CREATE TABLE IF NOT EXISTS filemeta (
#   dirhash     BIGINT,
#   name        VARCHAR(65535),
#   directory   VARCHAR(65535),
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
username=""
password=""
# This changes the data layout. Only add new directories. Removing/Updating will cause data loss.
superLargeDirectories = []

[hbase]
enabled = false
zkquorum = ""
table = "seaweedfs"

[redis2]
enabled = false
address  = "localhost:6379"
password = ""
database = 0
# This changes the data layout. Only add new directories. Removing/Updating will cause data loss.
superLargeDirectories = []

[redis_cluster2]
enabled = false
addresses = [
    "localhost:30001",
    "localhost:30002",
    "localhost:30003",
    "localhost:30004",
    "localhost:30005",
    "localhost:30006",
]
password = ""
# allows reads from slave servers or the master, but all writes still go to the master
readOnly = true
# automatically use the closest Redis server for reads
routeByLatency = true
# This changes the data layout. Only add new directories. Removing/Updating will cause data loss.
superLargeDirectories = []

[etcd]
enabled = false
servers = "localhost:2379"
timeout = "3s"

[mongodb]
enabled = false
uri = "mongodb://localhost:27017"
option_pool_size = 0
database = "seaweedfs"

[elastic7]
enabled = false
servers = [
    "http://localhost1:9200",
    "http://localhost2:9200",
    "http://localhost3:9200",
]
username = ""
password = ""
sniff_enabled = false
healthcheck_enabled = false
# increase the value is recommend, be sure the value in Elastic is greater or equal here
index.max_result_window = 10000



##########################
##########################
# To add path-specific filer store:
#
# 1. Add a name following the store type separated by a dot ".". E.g., cassandra.tmp
# 2. Add a location configuraiton. E.g., location = "/tmp/"
# 3. Copy and customize all other configurations. 
#     Make sure they are not the same if using the same store type!
# 4. Set enabled to true
#
# The following is just using cassandra as an example
##########################
[redis2.tmp]
enabled = false
location = "/tmp/"
address  = "localhost:6379"
password = ""
database = 1

`

	NOTIFICATION_TOML_EXAMPLE = `
# A sample TOML config file for SeaweedFS filer store
# Used by both "weed filer" or "weed server -filer" and "weed filer.replicate"
# Put this file to one of the location, with descending priority
#    ./notification.toml
#    $HOME/.seaweedfs/notification.toml
#    /etc/seaweedfs/notification.toml

####################################################
# notification
# send and receive filer updates for each file to an external message queue
####################################################
[notification.log]
# this is only for debugging perpose and does not work with "weed filer.replicate"
enabled = false


[notification.kafka]
enabled = false
hosts = [
  "localhost:9092"
]
topic = "seaweedfs_filer"
offsetFile = "./last.offset"
offsetSaveIntervalSeconds = 10


[notification.aws_sqs]
# experimental, let me know if it works
enabled = false
aws_access_key_id     = ""        # if empty, loads from the shared credentials file (~/.aws/credentials).
aws_secret_access_key = ""        # if empty, loads from the shared credentials file (~/.aws/credentials).
region = "us-east-2"
sqs_queue_name = "my_filer_queue" # an existing queue name


[notification.google_pub_sub]
# read credentials doc at https://cloud.google.com/docs/authentication/getting-started
enabled = false
google_application_credentials = "/path/to/x.json" # path to json credential file
project_id = ""                       # an existing project id
topic = "seaweedfs_filer_topic"       # a topic, auto created if does not exists

[notification.gocdk_pub_sub]
# The Go Cloud Development Kit (https://gocloud.dev).
# PubSub API (https://godoc.org/gocloud.dev/pubsub).
# Supports AWS SNS/SQS, Azure Service Bus, Google PubSub, NATS and RabbitMQ.
enabled = false
# This URL will Dial the RabbitMQ server at the URL in the environment
# variable RABBIT_SERVER_URL and open the exchange "myexchange".
# The exchange must have already been created by some other means, like
# the RabbitMQ management plugin.
topic_url = "rabbit://myexchange"
sub_url = "rabbit://myqueue"
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
# all files under this directory tree are replicated.
# this is not a directory on your hard drive, but on your filer.
# i.e., all files with this "prefix" are sent to notification message queue.
directory = "/buckets"

[sink.filer]
enabled = false
grpcAddress = "localhost:18888"
# all replicated files are under this directory tree
# this is not a directory on your hard drive, but on your filer.
# i.e., all received files will be "prefixed" to this directory.
directory = "/backup"
replication = ""
collection = ""
ttlSec = 0

[sink.s3]
# read credentials doc at https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sessions.html
# default loads credentials from the shared credentials file (~/.aws/credentials).
enabled = false
aws_access_key_id     = ""     # if empty, loads from the shared credentials file (~/.aws/credentials).
aws_secret_access_key = ""     # if empty, loads from the shared credentials file (~/.aws/credentials).
region = "us-east-2"
bucket = "your_bucket_name"    # an existing bucket
directory = "/"                # destination directory
endpoint = ""

[sink.google_cloud_storage]
# read credentials doc at https://cloud.google.com/docs/authentication/getting-started
enabled = false
google_application_credentials = "/path/to/x.json" # path to json credential file
bucket = "your_bucket_seaweedfs"    # an existing bucket
directory = "/"                     # destination directory

[sink.azure]
# experimental, let me know if it works
enabled = false
account_name = ""
account_key  = ""
container = "mycontainer"      # an existing container
directory = "/"                # destination directory

[sink.backblaze]
enabled = false
b2_account_id = ""
b2_master_application_key  = ""
bucket = "mybucket"            # an existing bucket
directory = "/"                # destination directory

`

	SECURITY_TOML_EXAMPLE = `
# Put this file to one of the location, with descending priority
#    ./security.toml
#    $HOME/.seaweedfs/security.toml
#    /etc/seaweedfs/security.toml
# this file is read by master, volume server, and filer

# the jwt signing key is read by master and volume server.
# a jwt defaults to expire after 10 seconds.
[jwt.signing]
key = ""
expires_after_seconds = 10           # seconds

# jwt for read is only supported with master+volume setup. Filer does not support this mode.
[jwt.signing.read]
key = ""
expires_after_seconds = 10           # seconds

# all grpc tls authentications are mutual
# the values for the following ca, cert, and key are paths to the PERM files.
# the host name is not checked, so the PERM files can be shared.
[grpc]
ca = ""

[grpc.volume]
cert = ""
key  = ""

[grpc.master]
cert = ""
key  = ""

[grpc.filer]
cert = ""
key  = ""

[grpc.msg_broker]
cert = ""
key  = ""

# use this for any place needs a grpc client
# i.e., "weed backup|benchmark|filer.copy|filer.replicate|mount|s3|upload"
[grpc.client]
cert = ""
key  = ""


# volume server https options
# Note: work in progress!
#     this does not work with other clients, e.g., "weed filer|mount" etc, yet.
[https.client]
enabled = true
[https.volume]
cert = ""
key  = ""


`

	MASTER_TOML_EXAMPLE = `
# Put this file to one of the location, with descending priority
#    ./master.toml
#    $HOME/.seaweedfs/master.toml
#    /etc/seaweedfs/master.toml
# this file is read by master

[master.maintenance]
# periodically run these scripts are the same as running them from 'weed shell'
scripts = """
  lock
  ec.encode -fullPercent=95 -quietFor=1h
  ec.rebuild -force
  ec.balance -force
  volume.balance -force
  volume.fix.replication
  unlock
"""
sleep_minutes = 17          # sleep minutes between each script execution

[master.filer]
default = "localhost:8888"    # used by maintenance scripts if the scripts needs to use fs related commands


[master.sequencer]
type = "raft"     # Choose [raft|etcd] type for storing the file id sequence
# when sequencer.type = etcd, set listen client urls of etcd cluster that store file id sequence
# example : http://127.0.0.1:2379,http://127.0.0.1:2389
sequencer_etcd_urls = "http://127.0.0.1:2379"


# configurations for tiered cloud storage
# old volumes are transparently moved to cloud for cost efficiency
[storage.backend]
	[storage.backend.s3.default]
	enabled = false
	aws_access_key_id     = ""     # if empty, loads from the shared credentials file (~/.aws/credentials).
	aws_secret_access_key = ""     # if empty, loads from the shared credentials file (~/.aws/credentials).
	region = "us-east-2"
	bucket = "your_bucket_name"    # an existing bucket
	endpoint = ""

# create this number of logical volumes if no more writable volumes
# count_x means how many copies of data.
# e.g.:
#   000 has only one copy, copy_1
#   010 and 001 has two copies, copy_2
#   011 has only 3 copies, copy_3
[master.volume_growth]
copy_1 = 7                # create 1 x 7 = 7 actual volumes
copy_2 = 6                # create 2 x 6 = 12 actual volumes
copy_3 = 3                # create 3 x 3 = 9 actual volumes
copy_other = 1            # create n x 1 = n actual volumes

# configuration flags for replication
[master.replication]
# any replication counts should be considered minimums. If you specify 010 and
# have 3 different racks, that's still considered writable. Writes will still
# try to replicate to all available volumes. You should only use this option
# if you are doing your own replication or periodic sync of volumes.
treat_replication_as_minimums = false

`
)
