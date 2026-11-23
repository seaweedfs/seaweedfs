package plugin

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// cloneClusterContext copies field by field, so a field added to the message
// and not to the copy would vanish from every scheduled request.
func TestCloneClusterContextKeepsEveryField(t *testing.T) {
	in := &plugin_pb.ClusterContext{}
	m := in.ProtoReflect()
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch {
		case fd.IsMap():
			m.Mutable(fd).Map().Set(protoreflect.ValueOfString("k").MapKey(), protoreflect.ValueOfString("v"))
		case fd.IsList() && fd.Kind() == protoreflect.StringKind:
			m.Mutable(fd).List().Append(protoreflect.ValueOfString(string(fd.Name())))
		default:
			t.Fatalf("field %s has a shape this test does not fill; extend it", fd.Name())
		}
	}
	if out := cloneClusterContext(in); !proto.Equal(in, out) {
		t.Fatalf("clone dropped a field:\n in: %v\nout: %v", in, out)
	}
}
