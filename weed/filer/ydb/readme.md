## YDB

database: https://github.com/ydb-platform/ydb
go driver: https://github.com/ydb-platform/ydb-go-sdk

options:

```
[ydb]
enabled=true
prefix="seaweedfs"
useBucketPrefix=true
coonectionUrl=grpcs://ydb-ru.yandex.net:2135/?database=/ru/home/username/db
```

get ydb types
```
ydbgen -dir weed/filer/ydb
```

i test using this dev database:
`make dev_ydb`
