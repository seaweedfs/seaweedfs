package s3api

import (
	"encoding/xml"
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const maxPublicAccessBlockConfigurationSize = 4 << 10

// PublicAccessBlockConfiguration is a bucket's S3 Block Public Access setting.
type PublicAccessBlockConfiguration struct {
	XMLNS                 string     `xml:"xmlns,attr,omitempty"`
	XMLName               xml.Name   `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool       `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool       `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool       `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool       `xml:"RestrictPublicBuckets"`
	Unknown               []xml.Name `xml:",any"`
}

func parsePublicAccessBlock(data []byte) (*PublicAccessBlockConfiguration, error) {
	var config PublicAccessBlockConfiguration
	if err := xml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	if len(config.Unknown) > 0 {
		return nil, fmt.Errorf("unknown element %s", config.Unknown[0].Local)
	}
	config.XMLNS, config.XMLName = "", xml.Name{}
	return &config, nil
}

func (c *PublicAccessBlockConfiguration) blocksPublicAcls() bool {
	return c != nil && c.BlockPublicAcls
}

func (c *PublicAccessBlockConfiguration) ignoresPublicAcls() bool {
	return c != nil && c.IgnorePublicAcls
}

// hasPublicGrant reports whether any grant names the AllUsers or
// AuthenticatedUsers group, which is what S3 treats as a public ACL.
func hasPublicGrant(grants []*s3.Grant) bool {
	for _, grant := range grants {
		if grant == nil || grant.Grantee == nil || grant.Grantee.URI == nil {
			continue
		}
		switch *grant.Grantee.URI {
		case s3_constants.GranteeGroupAllUsers, s3_constants.GranteeGroupAuthenticatedUsers:
			return true
		}
	}
	return false
}

// rejectPublicGrants denies grants that would make an object or bucket
// public when the bucket blocks public ACLs.
func rejectPublicGrants(config *BucketConfig, grants []*s3.Grant) s3err.ErrorCode {
	if config != nil && config.PublicAccessBlock.blocksPublicAcls() && hasPublicGrant(grants) {
		return s3err.ErrAccessDenied
	}
	return s3err.ErrNone
}
