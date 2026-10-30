package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/cors"
)

func TestCorsRuleProtoRoundTripMaxAge(t *testing.T) {
	unset := corsRuleFromProto(corsRuleToProto(cors.CORSRule{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}))
	if unset.MaxAgeSeconds != nil {
		t.Errorf("unset MaxAgeSeconds came back as %d", *unset.MaxAgeSeconds)
	}

	for _, age := range []int{0, 3000} {
		age := age
		got := corsRuleFromProto(corsRuleToProto(cors.CORSRule{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}, MaxAgeSeconds: &age}))
		if got.MaxAgeSeconds == nil || *got.MaxAgeSeconds != age {
			t.Errorf("MaxAgeSeconds %d came back as %v", age, got.MaxAgeSeconds)
		}
	}
}
