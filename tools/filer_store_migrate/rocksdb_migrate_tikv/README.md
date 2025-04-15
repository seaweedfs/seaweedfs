# filer store migrate tool for SeaweedFS
1. Build docker image
```
cd seaweedfs/
docker build -t vdm-registry.cn-hangzhou.cr.aliyuncs.com/udm-dev/filer-migrate:0.7 -f tools/filer_store_migrate/rocksdb_migrate_tikv/Dockerfile ./
```
2. Prepare docker compose file
Refer to tools/filer_store_migrate/rocksdb_migrate_tikv/migrate-compose.yml

3. Enter docker and run migrate command
```
cd /app
./migrate -filer 192.168.2.71:8888 -config=./migrate_filer.toml
```