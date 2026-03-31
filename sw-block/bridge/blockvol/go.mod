module github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol

go 1.23.0

require github.com/seaweedfs/seaweedfs/sw-block/engine/replication v0.0.0

replace github.com/seaweedfs/seaweedfs/sw-block/engine/replication => ../../engine/replication
