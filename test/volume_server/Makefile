.PHONY: test-volume-server test-volume-server-short test-volume-server-rust-smoke

test-volume-server:
	go test ./test/volume_server/... -v

test-volume-server-short:
	go test ./test/volume_server/... -short -v

test-volume-server-rust-smoke:
	VOLUME_SERVER_IMPL=rust go test ./test/volume_server/http ./test/volume_server/grpc -run 'TestAdminStatusAndHealthz|TestUploadReadRangeHeadDeleteRoundTrip|TestStateAndStatusRPCs|TestVolumeSyncStatusAndReadVolumeFileStatus' -v
