## YDB

database: https://github.com/ydb-platform/ydb
go driver: https://github.com/ydb-platform/ydb-go-sdk

options:

```
[ydb]
enabled=true
db_name="seaweedfs"
servers=["http://localhost:8529"]
#basic auth
user="root"
pass="test"

# tls settings
insecure_skip_verify=true
```

get ydb types
```
ydbgen -dir weed/filer/ydb
```

i test using this dev database:
`make dev_ydb`
