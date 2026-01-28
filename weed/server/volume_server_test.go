package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

func TestMaintenanceMode(t *testing.T) {
	testCases := []struct {
		name         string
		pb           *volume_server_pb.VolumeServerState
		want         bool
		wantCheckErr string
	}{
		{
			name:         "non-initialized state",
			pb:           nil,
			want:         false,
			wantCheckErr: "",
		},
		{
			name: "maintenance mode disabled",
			pb: &volume_server_pb.VolumeServerState{
				Maintenance: false,
			},
			want:         false,
			wantCheckErr: "",
		},
		{
			name: "maintenance mode enabled",
			pb: &volume_server_pb.VolumeServerState{
				Maintenance: true,
			},
			want:         true,
			wantCheckErr: "volume server test_1234 is in maintenance mode",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vs := VolumeServer{
				store: &storage.Store{
					Id: "test_1234",
					State: &storage.State{
						FilePath: "/some/path.pb",
						Pb:       tc.pb,
					},
				},
			}

			if got, want := vs.MaintenanceMode(), tc.want; got != want {
				t.Errorf("MaintenanceMode() returned %v, want %v", got, want)
			}

			err, wantErrStr := vs.CheckMaintenanceMode(), tc.wantCheckErr
			if err != nil {
				if wantErrStr == "" {
					t.Errorf("CheckMaintenanceMode() returned error %v, want nil", err)
				}
				if errStr := err.Error(); errStr != wantErrStr {
					t.Errorf("CheckMaintenanceMode() returned error %q, want %q", errStr, wantErrStr)
				}
			} else {
				if wantErrStr != "" {
					t.Errorf("CheckMaintenanceMode() returned no error, want %q", wantErrStr)
				}
			}
		})
	}
}
