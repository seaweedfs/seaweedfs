module github.com/chrislusf/seaweedfs

go 1.13

require (
	cloud.google.com/go v0.44.3
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Shopify/sarama v1.23.1
	github.com/aws/aws-sdk-go v1.23.13
	github.com/chrislusf/raft v0.0.0-20190225081310-10d6e2182d92
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/disintegration/imaging v1.6.1
	github.com/dustin/go-humanize v1.0.0
	github.com/gabriel-vasile/mimetype v0.3.17
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20190829130954-e163eff7a8c6
	github.com/golang/protobuf v1.3.2
	github.com/google/btree v1.0.0
	github.com/gorilla/mux v1.7.3
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f
	github.com/joeslay/seaweedfs v0.0.0-20190912104409-d8c34b032fb6 // indirect
	github.com/karlseguin/ccache v2.0.3+incompatible
	github.com/klauspost/crc32 v1.2.0
	github.com/klauspost/reedsolomon v1.9.2
	github.com/kurin/blazer v0.5.3
	github.com/lib/pq v1.2.0
	github.com/peterh/liner v1.1.0
	github.com/prometheus/client_golang v1.1.0
	github.com/rakyll/statik v0.1.6
	github.com/rwcarlsen/goexif v0.0.0-20190401172101-9e8deecbddbd
	github.com/satori/go.uuid v1.2.0
	github.com/seaweedfs/fuse v0.0.0-20190510212405-310228904eff
	github.com/spf13/viper v1.4.0
	github.com/syndtr/goleveldb v1.0.0
	github.com/willf/bloom v2.0.3+incompatible
	go.etcd.io/etcd v3.3.15+incompatible
	gocloud.dev v0.16.0
	gocloud.dev/pubsub/natspubsub v0.16.0
	gocloud.dev/pubsub/rabbitpubsub v0.16.0
	golang.org/x/net v0.0.0-20190827160401-ba9fcec4b297
	golang.org/x/sys v0.0.0-20190830142957-1e83adbbebd0
	golang.org/x/tools v0.0.0-20190830223141-573d9926052a
	google.golang.org/api v0.9.0
	google.golang.org/grpc v1.23.0
)

replace github.com/satori/go.uuid v1.2.0 => github.com/satori/go.uuid v0.0.0-20181028125025-b2ce2384e17b
