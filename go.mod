module github.com/seaweedfs/seaweedfs

go 1.24

toolchain go1.24.1

require (
	cloud.google.com/go v0.121.1 // indirect
	cloud.google.com/go/pubsub v1.49.0
	cloud.google.com/go/storage v1.55.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/Shopify/sarama v1.38.1
	github.com/aws/aws-sdk-go v1.55.7
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bwmarrin/snowflake v0.3.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230111030713-bf00bc1b83b6 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/stats v0.0.0-20151006221625-1b76add642e4
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-redsync/redsync/v4 v4.13.0
	github.com/go-sql-driver/mysql v1.9.3
	github.com/go-zookeeper/zk v1.0.3 // indirect
	github.com/gocql/gocql v1.7.0
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3
	github.com/google/uuid v1.6.0
	github.com/google/wire v0.6.0 // indirect
	github.com/googleapis/gax-go/v2 v2.14.2 // indirect
	github.com/gorilla/mux v1.8.1
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jinzhu/copier v0.4.0
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12
	github.com/karlseguin/ccache/v2 v2.0.8
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/reedsolomon v1.12.4
	github.com/kurin/blazer v0.5.3
	github.com/lib/pq v1.10.9
	github.com/linxGnu/grocksdb v1.10.1
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-ieproxy v0.0.11 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/olivere/elastic/v7 v7.0.32
	github.com/peterh/liner v1.2.2
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/posener/complete v1.2.3
	github.com/pquerna/cachecontrol v0.2.0
	github.com/prometheus/client_golang v1.22.0
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.64.0 // indirect
	github.com/prometheus/procfs v0.16.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/seaweedfs/goexif v1.0.3
	github.com/seaweedfs/raft v1.1.3
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/afero v1.12.0 // indirect
	github.com/spf13/cast v1.7.1 // indirect
	github.com/spf13/viper v1.20.1
	github.com/stretchr/testify v1.10.0
	github.com/stvp/tempredis v0.0.0-20181119212430-b82af8480203
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/tidwall/gjson v1.18.0
	github.com/tidwall/match v1.1.1
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tsuna/gohbase v0.0.0-20201125011725-348991136365
	github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43
	github.com/valyala/bytebufferpool v1.0.0
	github.com/viant/ptrie v1.0.1
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.etcd.io/etcd/client/v3 v3.6.1
	go.mongodb.org/mongo-driver v1.17.4
	go.opencensus.io v0.24.0 // indirect
	gocloud.dev v0.42.0
	gocloud.dev/pubsub/natspubsub v0.42.0
	gocloud.dev/pubsub/rabbitpubsub v0.41.0
	golang.org/x/crypto v0.39.0
	golang.org/x/exp v0.0.0-20250606033433-dcc06ee1d476
	golang.org/x/image v0.28.0
	golang.org/x/net v0.41.0
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.33.0
	golang.org/x/text v0.26.0 // indirect
	golang.org/x/tools v0.34.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.239.0
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/grpc v1.73.0
	google.golang.org/protobuf v1.36.6
	gopkg.in/inf.v0 v0.9.1 // indirect
	modernc.org/b v1.0.0 // indirect
	modernc.org/mathutil v1.7.1
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.38.0
	modernc.org/strutil v1.2.1
)

require (
	github.com/Jille/raft-grpc-transport v1.6.1
	github.com/a-h/templ v0.3.906
	github.com/arangodb/go-driver v1.6.6
	github.com/armon/go-metrics v0.4.1
	github.com/aws/aws-sdk-go-v2 v1.36.5
	github.com/aws/aws-sdk-go-v2/config v1.29.17
	github.com/aws/aws-sdk-go-v2/credentials v1.17.70
	github.com/aws/aws-sdk-go-v2/service/s3 v1.82.0
	github.com/cognusion/imaging v1.0.2
	github.com/fluent/fluent-logger-golang v1.10.0
	github.com/getsentry/sentry-go v0.33.0
	github.com/gin-contrib/sessions v1.0.4
	github.com/gin-gonic/gin v1.10.1
	github.com/golang-jwt/jwt/v5 v5.2.2
	github.com/google/flatbuffers/go v0.0.0-20230108230133-3b8644d32c50
	github.com/hanwen/go-fuse/v2 v2.8.0
	github.com/hashicorp/raft v1.7.3
	github.com/hashicorp/raft-boltdb/v2 v2.3.1
	github.com/minio/crc64nvme v1.0.2
	github.com/orcaman/concurrent-map/v2 v2.0.1
	github.com/parquet-go/parquet-go v0.25.1
	github.com/pkg/sftp v1.13.9
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/rclone/rclone v1.70.2
	github.com/rdleal/intervalst v1.5.0
	github.com/redis/go-redis/v9 v9.10.0
	github.com/schollz/progressbar/v3 v3.18.0
	github.com/shirou/gopsutil/v3 v3.24.5
	github.com/tarantool/go-tarantool/v2 v2.3.2
	github.com/tikv/client-go/v2 v2.0.7
	github.com/ydb-platform/ydb-go-sdk-auth-environ v0.5.0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.111.3
	go.etcd.io/etcd/client/pkg/v3 v3.6.1
	go.uber.org/atomic v1.11.0
	golang.org/x/sync v0.15.0
	google.golang.org/grpc/security/advancedtls v1.0.0
)

require (
	cel.dev/expr v0.23.0 // indirect
	cloud.google.com/go/auth v0.16.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.7.0 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.18.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.10.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azfile v1.5.1 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20221128193559-754e69321358 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.4.2 // indirect
	github.com/Files-com/files-sdk-go/v3 v3.2.173 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.27.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.51.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.51.0 // indirect
	github.com/IBM/go-sdk-core/v5 v5.20.0 // indirect
	github.com/Max-Sum/base32768 v0.0.0-20230304063302-18e6ce5945fd // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/ProtonMail/bcrypt v0.0.0-20211005172633-e235017c1baf // indirect
	github.com/ProtonMail/gluon v0.17.1-0.20230724134000-308be39be96e // indirect
	github.com/ProtonMail/go-crypto v1.3.0 // indirect
	github.com/ProtonMail/go-mime v0.0.0-20230322103455-7d82a3887f2f // indirect
	github.com/ProtonMail/go-srp v0.0.7 // indirect
	github.com/ProtonMail/gopenpgp/v2 v2.9.0 // indirect
	github.com/PuerkitoBio/goquery v1.10.3 // indirect
	github.com/abbot/go-http-auth v0.4.0 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/appscode/go-querystring v0.0.0-20170504095604-0126cfb3f1dc // indirect
	github.com/arangodb/go-velocypack v0.0.0-20200318135517-5af53c29c67e // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.11 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.32 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.77 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.34.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.38.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.34.0 // indirect
	github.com/aws/smithy-go v1.22.4 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/bradenaw/juniper v0.15.3 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/buengese/sgzip v0.1.1 // indirect
	github.com/bytedance/sonic v1.13.2 // indirect
	github.com/bytedance/sonic/loader v0.2.4 // indirect
	github.com/calebcase/tmpfile v1.0.3 // indirect
	github.com/chilts/sid v0.0.0-20190607042430-660e94789ec9 // indirect
	github.com/cloudflare/circl v1.6.1 // indirect
	github.com/cloudinary/cloudinary-go/v2 v2.10.0 // indirect
	github.com/cloudsoda/go-smb2 v0.0.0-20250228001242-d4c70e6251cc // indirect
	github.com/cloudsoda/sddl v0.0.0-20250224235906-926454e91efc // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/cncf/xds/go v0.0.0-20250326154945-ae57f3c0d45f // indirect
	github.com/colinmarc/hdfs/v2 v2.4.0 // indirect
	github.com/creasty/defaults v1.8.0 // indirect
	github.com/cronokirby/saferith v0.33.0 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dropbox/dropbox-sdk-go-unofficial/v6 v6.0.5 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/elastic/gosigar v0.14.2 // indirect
	github.com/emersion/go-message v0.18.2 // indirect
	github.com/emersion/go-vcard v0.0.0-20241024213814-c9703dde27ff // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/flynn/noise v1.1.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.9 // indirect
	github.com/geoffgarside/ber v1.2.0 // indirect
	github.com/gin-contrib/sse v1.0.0 // indirect
	github.com/go-chi/chi/v5 v5.2.2 // indirect
	github.com/go-darwin/apfs v0.0.0-20211011131704-f84b94dbf348 // indirect
	github.com/go-jose/go-jose/v4 v4.0.5 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/errors v0.22.1 // indirect
	github.com/go-openapi/strfmt v0.23.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.26.0 // indirect
	github.com/go-resty/resty/v2 v2.16.5 // indirect
	github.com/go-viper/mapstructure/v2 v2.3.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/gorilla/context v1.1.2 // indirect
	github.com/gorilla/schema v1.4.1 // indirect
	github.com/gorilla/securecookie v1.1.2 // indirect
	github.com/gorilla/sessions v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.7 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/henrybear327/Proton-API-Bridge v1.0.0 // indirect
	github.com/henrybear327/go-proton-api v1.0.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jlaffaye/ftp v0.2.1-0.20240918233326-1b970516f5d3 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jtolio/noiseconn v0.0.0-20231127013910-f6d9ecbf1de7 // indirect
	github.com/jzelinskie/whirlpool v0.0.0-20201016144138-0675e54bb004 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/koofr/go-httpclient v0.0.0-20240520111329-e20f8f203988 // indirect
	github.com/koofr/go-koofrclient v0.0.0-20221207135200-cbd7fc9ad6a6 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lanrat/extsort v1.0.2 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lpar/date v1.0.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20250317134145-8bc96cf8fc35 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nats-io/nats.go v1.40.1 // indirect
	github.com/nats-io/nkeys v0.4.10 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/ncw/swift/v2 v2.0.4 // indirect
	github.com/nxadm/tail v1.4.11 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/onsi/ginkgo/v2 v2.23.3 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/oracle/oci-go-sdk/v65 v65.93.0 // indirect
	github.com/panjf2000/ants/v2 v2.11.3 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pengsrc/go-shared v0.2.1-0.20190131101655-1999055a4a14 // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c // indirect
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c // indirect
	github.com/pingcap/kvproto v0.0.0-20230403051650-e166ae588106 // indirect
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pkg/xattr v0.4.10 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/putdotio/go-putio/putio v0.0.0-20200123120452-16d982cac2b8 // indirect
	github.com/relvacode/iso8601 v1.6.0 // indirect
	github.com/rfjakob/eme v1.1.2 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/sabhiram/go-gitignore v0.0.0-20210923224102-525f6e181f06 // indirect
	github.com/sagikazarmark/locafero v0.7.0 // indirect
	github.com/samber/lo v1.50.0 // indirect
	github.com/shirou/gopsutil/v4 v4.25.5 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966 // indirect
	github.com/smartystreets/goconvey v1.8.1 // indirect
	github.com/sony/gobreaker v1.0.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.24 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/spiffe/go-spiffe/v2 v2.5.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/t3rm1n4l/go-mega v0.0.0-20241213151442-a19cff0ec7b5 // indirect
	github.com/tarantool/go-iproto v1.1.0 // indirect
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a // indirect
	github.com/tikv/pd/client v0.0.0-20230329114254-1948c247c2b1 // indirect
	github.com/tinylib/msgp v1.3.0 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/unknwon/goconfig v1.0.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xanzy/ssh-agent v0.3.3 // indirect
	github.com/yandex-cloud/go-genproto v0.0.0-20211115083454-9ca41db5ed9e // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20241112172322-ea1f63298f77 // indirect
	github.com/ydb-platform/ydb-go-yc v0.12.1 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.6.1 // indirect
	github.com/yunify/qingstor-sdk-go/v3 v3.2.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.etcd.io/bbolt v1.4.0 // indirect
	go.etcd.io/etcd/api/v3 v3.6.1 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.36.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
	go.opentelemetry.io/otel v1.36.0 // indirect
	go.opentelemetry.io/otel/metric v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.36.0 // indirect
	go.opentelemetry.io/otel/trace v1.36.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/arch v0.16.0 // indirect
	golang.org/x/term v0.32.0 // indirect
	golang.org/x/time v0.12.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250603155806-513f23925822 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/validator.v2 v2.0.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/libc v1.65.10 // indirect
	moul.io/http2curl/v2 v2.3.0 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
	storj.io/common v0.0.0-20250605163628-70ca83b6228e // indirect
	storj.io/drpc v0.0.35-0.20250513201419-f7819ea69b55 // indirect
	storj.io/eventkit v0.0.0-20250410172343-61f26d3de156 // indirect
	storj.io/infectious v0.0.2 // indirect
	storj.io/picobuf v0.0.4 // indirect
	storj.io/uplink v1.13.1 // indirect
)

// replace github.com/seaweedfs/raft => /Users/chrislu/go/src/github.com/seaweedfs/raft
