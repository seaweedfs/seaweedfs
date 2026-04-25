package parquet_pushdown

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

// A predicate paired with a SchemaId is the well-formed shape; a
// well-formed predicate without SchemaId must be rejected (deeper
// catalog binding lands in M3).
func TestValidateRequest_PredicateRequiresSchemaId(t *testing.T) {
	req := validRequest()
	req.PredicateKind = pb.PredicateKind_PREDICATE_KIND_SUBSTRAIT
	req.Predicate = []byte("bound expression bytes")
	req.SchemaId = 7

	if err := validateRequest(req); err != nil {
		t.Fatalf("predicate + schema_id should pass, got %v", err)
	}

	req.SchemaId = 0
	err := validateRequest(req)
	if err == nil {
		t.Fatal("predicate without schema_id should be rejected")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}
