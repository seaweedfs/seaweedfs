package pb

import (
	"fmt"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/golang/protobuf/jsonpb"
)

func TestJsonpMarshalUnmarshal(t *testing.T) {

	tv := &volume_server_pb.TieredVolume{
		BackendType: "aws",
		BackendName: "",
		Version:     12,
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
		"backendName":"temp",
		"version":12
	}`

	tv1 := &volume_server_pb.TieredVolume{}
	if err := jsonpb.UnmarshalString(rawJson, tv1); err != nil {
		fmt.Printf("unmarshal eror: %v\n", err)
	}

	fmt.Printf("unmarshalled: %+v\n", tv1)

}
