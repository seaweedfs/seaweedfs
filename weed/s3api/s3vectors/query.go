package s3vectors

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	searchModeHeader        = s3_constants.SeaweedFSVectorSearchMode
	excludedFragmentsHeader = "x-seaweedfs-vector-excluded-fragments"
	dataExcludedHeader      = "x-seaweedfs-vector-data-excluded"
)

func searchMode(r *http.Request) (bool, error) {
	values := r.Header.Values(searchModeHeader)
	if len(values) == 0 {
		return false, nil
	}
	if len(values) != 1 || (values[0] != "all" && values[0] != "indexed-only") {
		return false, fmt.Errorf("%s must be all or indexed-only", searchModeHeader)
	}
	return values[0] == "indexed-only", nil
}

func (s *Server) queryVectors(w http.ResponseWriter, r *http.Request, req *vector_pb.QueryVectorsRequest, indexedOnly bool) (*vector_pb.QueryVectorsResponse, error) {
	if !indexedOnly {
		return s.vectors.QueryVectors(r.Context(), req)
	}
	var headers metadata.MD
	resp, err := s.vectors.QueryIndexedVectors(r.Context(), req, grpc.Header(&headers))
	if status.Code(err) == codes.Unimplemented {
		return nil, status.Error(codes.Unavailable, "indexed-only search requires an updated vector engine")
	}
	if err != nil {
		return nil, err
	}
	mode := headers.Get(searchModeHeader)
	excluded := headers.Get(excludedFragmentsHeader)
	if len(mode) != 1 || mode[0] != "indexed-only" || len(excluded) != 1 {
		return nil, status.Error(codes.Unavailable, "vector engine did not acknowledge indexed-only search")
	}
	count, err := strconv.ParseUint(excluded[0], 10, 64)
	if err != nil {
		return nil, status.Error(codes.Internal, "invalid indexed-only coverage response")
	}
	w.Header().Set(searchModeHeader, mode[0])
	w.Header().Set(excludedFragmentsHeader, excluded[0])
	w.Header().Set(dataExcludedHeader, strconv.FormatBool(count > 0))
	return resp, nil
}
