package s3api

import (
	"encoding/xml"
)

const maxPublicAccessBlockConfigurationSize = 4 << 10

// PublicAccessBlockConfiguration is a bucket's S3 Block Public Access setting.
type PublicAccessBlockConfiguration struct {
	XMLNS                 string   `xml:"xmlns,attr,omitempty"`
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

func parsePublicAccessBlock(data []byte) (*PublicAccessBlockConfiguration, error) {
	var config PublicAccessBlockConfiguration
	if err := xml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	config.XMLNS = ""
	return &config, nil
}

func (c *PublicAccessBlockConfiguration) blocksPublicAcls() bool {
	return c != nil && c.BlockPublicAcls
}

func (c *PublicAccessBlockConfiguration) ignoresPublicAcls() bool {
	return c != nil && c.IgnorePublicAcls
}
