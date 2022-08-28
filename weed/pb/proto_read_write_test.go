package pb

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestJsonpMarshalUnmarshal(t *testing.T) {

	tv := &volume_server_pb.RemoteFile{
		BackendType: "aws",
		BackendId:   "",
		FileSize:    12,
	}

	m := jsonpb.Marshaler{
		EmitDefaults: true,
		Indent:       "  ",
	}

	if text, err := m.MarshalToString(tv); err != nil {
		fmt.Printf("marshal eror: %v\n", err)
	} else {
		fmt.Printf("marshalled: %s\n", text)
	}

	rawJson := `{
		"backendType":"aws",
		"backendId":"temp",
		"fileSize":12
	}`

	tv1 := &volume_server_pb.RemoteFile{}
	if err := jsonpb.UnmarshalString(rawJson, tv1); err != nil {
		fmt.Printf("unmarshal error: %v\n", err)
	}

	fmt.Printf("unmarshalled: %+v\n", tv1)

}
