package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
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

// storedPublicAccessBlock reads a bucket entry's public access block. An
// unreadable one blocks all public access.
func storedPublicAccessBlock(bucket string, extended map[string][]byte) *PublicAccessBlockConfiguration {
	data, exists := extended[s3_constants.ExtPublicAccessBlockKey]
	if !exists || len(data) == 0 {
		return nil
	}
	pab, err := parsePublicAccessBlock(data)
	if err != nil {
		glog.Errorf("bucket %s public access block, blocking all public access: %v", bucket, err)
		return &PublicAccessBlockConfiguration{BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true}
	}
	return pab
}

func (c *PublicAccessBlockConfiguration) blocksPublicAcls() bool {
	return c != nil && c.BlockPublicAcls
}

func (c *PublicAccessBlockConfiguration) ignoresPublicAcls() bool {
	return c != nil && c.IgnorePublicAcls
}

func (c *PublicAccessBlockConfiguration) blocksPublicPolicy() bool {
	return c != nil && c.BlockPublicPolicy
}

func (c *PublicAccessBlockConfiguration) restrictsPublicBuckets() bool {
	return c != nil && c.RestrictPublicBuckets
}

// restrictsPublicBuckets fails closed: a bucket whose configuration cannot be
// read is treated as restricted.
func (s3a *S3ApiServer) restrictsPublicBuckets(bucket string) bool {
	config, errCode := s3a.getBucketConfig(bucket)
	return errCode != s3err.ErrNone || config.PublicAccessBlock.restrictsPublicBuckets()
}

var errPublicPolicyBlocked = errors.New("bucket blocks public policies")

// restrictingConditionKeys are the condition keys that, with fixed values,
// keep an Allow to every principal from making a policy public.
var restrictingConditionKeys = map[string]bool{
	"aws:sourceip":              true,
	"aws:sourcevpc":             true,
	"aws:sourcevpce":            true,
	"aws:sourcearn":             true,
	"aws:sourceaccount":         true,
	"aws:sourceowner":           true,
	"aws:principalaccount":      true,
	"aws:principalarn":          true,
	"aws:principalorgid":        true,
	"aws:principalorgpaths":     true,
	"aws:userid":                true,
	"s3:dataaccesspointarn":     true,
	"s3:dataaccesspointaccount": true,
}

// isPublicPolicy reports whether a bucket policy grants access to anyone, the
// way S3 Block Public Access judges it: an Allow to a wildcard principal (or
// to everyone but a NotPrincipal) that no fixed-value restricting condition
// narrows.
func isPublicPolicy(doc *policy_engine.PolicyDocument) bool {
	if doc == nil {
		return false
	}
	for _, statement := range doc.Statement {
		if statement.Effect != policy_engine.PolicyEffectAllow {
			continue
		}
		if statement.NotPrincipal == nil && !hasPublicPrincipal(statement.Principal.Strings()) {
			continue
		}
		if !hasRestrictingCondition(statement.Condition) {
			return true
		}
	}
	return false
}

func hasPublicPrincipal(principals []string) bool {
	if len(principals) == 0 {
		return true
	}
	for _, principal := range principals {
		if principal != "" && (!strings.ContainsAny(principal, "*?$") || hasFixedAccount(principal)) {
			continue
		}
		return true
	}
	return false
}

// hasFixedAccount reports whether an ARN names one account, whatever
// wildcards its resource part holds.
func hasFixedAccount(arn string) bool {
	parts := strings.SplitN(arn, ":", 6)
	return len(parts) == 6 && parts[0] == "arn" && parts[4] != "" && !strings.ContainsAny(parts[4], "*?$")
}

func hasRestrictingCondition(conditions policy_engine.PolicyConditions) bool {
	for operator, keys := range conditions {
		op := strings.ToLower(operator)
		if strings.HasSuffix(op, "ifexists") || strings.HasPrefix(op, "forallvalues:") || strings.Contains(op, "not") {
			continue
		}
		op = strings.TrimPrefix(op, "foranyvalue:")
		for key, values := range keys {
			key = strings.ToLower(key)
			if !restrictingConditionKeys[key] {
				continue
			}
			if fixedConditionValues(op, key, values.Strings()) {
				return true
			}
		}
	}
	return false
}

func fixedConditionValues(operator, key string, values []string) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if key == "aws:sourceip" {
			if operator != "ipaddress" {
				return false
			}
			if _, network, err := net.ParseCIDR(value); err == nil {
				ones, bits := network.Mask.Size()
				if bits == 128 && network.IP.To4() != nil {
					ones, bits = ones-96, 32
				}
				if (bits == 32 && ones < 8) || (bits == 128 && ones < 32) {
					return false
				}
			} else if net.ParseIP(value) == nil {
				return false
			}
			continue
		}
		if !strings.HasPrefix(operator, "string") && !strings.HasPrefix(operator, "arn") {
			return false
		}
		if value == "" || (strings.ContainsAny(value, "*?$") && !hasFixedAccount(value)) {
			return false
		}
	}
	return true
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
