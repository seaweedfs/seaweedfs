package s3api

import (
	"net/http"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"time"
	"context"
	"fmt"
)

func (s3a *S3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {

	var response ListAllMyBucketsResponse
	err := s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory: "/buckets",
		}

		glog.V(4).Infof("read directory: %v", request)
		resp, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return fmt.Errorf("list buckets: %v", err)
		}

		var buckets []ListAllMyBucketsEntry
		for _, entry := range resp.Entries {
			if entry.IsDirectory {
				buckets = append(buckets, ListAllMyBucketsEntry{
					Name:         entry.Name,
					CreationDate: time.Unix(entry.Attributes.Crtime, 0),
				})
			}
		}

		response = ListAllMyBucketsResponse{
			ListAllMyBucketsResponse: ListAllMyBucketsResult{
				Owner: CanonicalUser{
					ID:          "",
					DisplayName: "",
				},
				Buckets: ListAllMyBucketsList{
					Bucket: buckets,
				},
			},
		}

		return nil
	})

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}
