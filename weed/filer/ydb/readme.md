## YDB

database: https://github.com/ydb-platform/ydb

go driver: https://github.com/ydb-platform/ydb-go-sdk

options:

```
[ydb]
enabled=true
dsn=grpcs://ydb-ru.yandex.net:2135/?database=/ru/home/username/db
prefix="seaweedfs"
useBucketPrefix=true
poolSizeLimit=50
dialTimeOut = 10
```

Authenticate produced with one of next environment variables:
 * `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path/to/sa_key_file>` — used service account key file by path
 * `YDB_ANONYMOUS_CREDENTIALS="1"` — used for authenticate with anonymous access. Anonymous access needs for connect to testing YDB installation
 * `YDB_METADATA_CREDENTIALS="1"` — used metadata service for authenticate to YDB from yandex cloud virtual machine or from yandex function
 * `YDB_ACCESS_TOKEN_CREDENTIALS=<access_token>` — used for authenticate to YDB with short-life access token. For example, access token may be IAM token
 * `YDB_CONNECTION_STRING="grpcs://endpoint/?database=database"`

 * i test using this dev database:
`make dev_ydb`
