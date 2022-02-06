package s3api

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"time"
)

type AccessControlList struct {
	Grant []Grant `xml:"Grant,omitempty"`
}

type AccessControlPolicy struct {
	Owner             CanonicalUser     `xml:"Owner"`
	AccessControlList AccessControlList `xml:"AccessControlList"`
}

type AmazonCustomerByEmail struct {
	EmailAddress string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ EmailAddress"`
}

type BucketLoggingStatus struct {
	LoggingEnabled LoggingSettings `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LoggingEnabled,omitempty"`
}

type CanonicalUser struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

type CopyObject struct {
	SourceBucket                string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ SourceBucket"`
	SourceKey                   string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ SourceKey"`
	DestinationBucket           string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DestinationBucket"`
	DestinationKey              string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DestinationKey"`
	MetadataDirective           MetadataDirective `xml:"http://s3.amazonaws.com/doc/2006-03-01/ MetadataDirective,omitempty"`
	Metadata                    []MetadataEntry   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Metadata,omitempty"`
	AccessControlList           AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlList,omitempty"`
	CopySourceIfModifiedSince   time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfModifiedSince,omitempty"`
	CopySourceIfUnmodifiedSince time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfUnmodifiedSince,omitempty"`
	CopySourceIfMatch           []string          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfMatch,omitempty"`
	CopySourceIfNoneMatch       []string          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfNoneMatch,omitempty"`
	StorageClass                StorageClass      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ StorageClass,omitempty"`
	AWSAccessKeyId              string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp                   time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature                   string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential                  string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *CopyObject) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T CopyObject
	var layout struct {
		*T
		CopySourceIfModifiedSince   *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfModifiedSince,omitempty"`
		CopySourceIfUnmodifiedSince *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfUnmodifiedSince,omitempty"`
		Timestamp                   *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.CopySourceIfModifiedSince = (*xsdDateTime)(&layout.T.CopySourceIfModifiedSince)
	layout.CopySourceIfUnmodifiedSince = (*xsdDateTime)(&layout.T.CopySourceIfUnmodifiedSince)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *CopyObject) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T CopyObject
	var overlay struct {
		*T
		CopySourceIfModifiedSince   *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfModifiedSince,omitempty"`
		CopySourceIfUnmodifiedSince *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopySourceIfUnmodifiedSince,omitempty"`
		Timestamp                   *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.CopySourceIfModifiedSince = (*xsdDateTime)(&overlay.T.CopySourceIfModifiedSince)
	overlay.CopySourceIfUnmodifiedSince = (*xsdDateTime)(&overlay.T.CopySourceIfUnmodifiedSince)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type CopyObjectResponse struct {
	CopyObjectResult CopyObjectResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult"`
}

type CopyObjectResult struct {
	LastModified time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	ETag         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ETag"`
}

func (t *CopyObjectResult) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T CopyObjectResult
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *CopyObjectResult) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T CopyObjectResult
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

type CreateBucket struct {
	Bucket            string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	AccessControlList AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlList,omitempty"`
	AWSAccessKeyId    string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp         time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature         string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
}

func (t *CreateBucket) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T CreateBucket
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *CreateBucket) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T CreateBucket
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type CreateBucketConfiguration struct {
	LocationConstraint string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
}

type CreateBucketResponse struct {
	CreateBucketReturn CreateBucketResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CreateBucketReturn"`
}

type CreateBucketResult struct {
	BucketName string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ BucketName"`
}

type DeleteBucket struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *DeleteBucket) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T DeleteBucket
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *DeleteBucket) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T DeleteBucket
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type DeleteBucketResponse struct {
	DeleteBucketResponse Status `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteBucketResponse"`
}

type DeleteMarkerEntry struct {
	Key          string        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	VersionId    string        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ VersionId"`
	IsLatest     bool          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IsLatest"`
	LastModified time.Time     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	Owner        CanonicalUser `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Owner,omitempty"`
}

func (t *DeleteMarkerEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T DeleteMarkerEntry
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *DeleteMarkerEntry) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T DeleteMarkerEntry
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

type DeleteObject struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key            string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *DeleteObject) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T DeleteObject
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *DeleteObject) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T DeleteObject
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type DeleteObjectResponse struct {
	DeleteObjectResponse Status `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteObjectResponse"`
}

type GetBucketAccessControlPolicy struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *GetBucketAccessControlPolicy) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T GetBucketAccessControlPolicy
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *GetBucketAccessControlPolicy) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T GetBucketAccessControlPolicy
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type GetBucketAccessControlPolicyResponse struct {
	GetBucketAccessControlPolicyResponse AccessControlPolicy `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetBucketAccessControlPolicyResponse"`
}

type GetBucketLoggingStatus struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *GetBucketLoggingStatus) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T GetBucketLoggingStatus
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *GetBucketLoggingStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T GetBucketLoggingStatus
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type GetBucketLoggingStatusResponse struct {
	GetBucketLoggingStatusResponse BucketLoggingStatus `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetBucketLoggingStatusResponse"`
}

type GetObject struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key            string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	GetMetadata    bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetMetadata"`
	GetData        bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetData"`
	InlineData     bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InlineData"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *GetObject) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T GetObject
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *GetObject) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T GetObject
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type GetObjectAccessControlPolicy struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key            string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *GetObjectAccessControlPolicy) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T GetObjectAccessControlPolicy
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *GetObjectAccessControlPolicy) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T GetObjectAccessControlPolicy
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type GetObjectAccessControlPolicyResponse struct {
	GetObjectAccessControlPolicyResponse AccessControlPolicy `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetObjectAccessControlPolicyResponse"`
}

type GetObjectExtended struct {
	Bucket                                 string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key                                    string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	GetMetadata                            bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetMetadata"`
	GetData                                bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetData"`
	InlineData                             bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InlineData"`
	ByteRangeStart                         int64     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ByteRangeStart,omitempty"`
	ByteRangeEnd                           int64     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ByteRangeEnd,omitempty"`
	IfModifiedSince                        time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfModifiedSince,omitempty"`
	IfUnmodifiedSince                      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfUnmodifiedSince,omitempty"`
	IfMatch                                []string  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfMatch,omitempty"`
	IfNoneMatch                            []string  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfNoneMatch,omitempty"`
	ReturnCompleteObjectOnConditionFailure bool      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ReturnCompleteObjectOnConditionFailure,omitempty"`
	AWSAccessKeyId                         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp                              time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature                              string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential                             string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *GetObjectExtended) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T GetObjectExtended
	var layout struct {
		*T
		IfModifiedSince   *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfModifiedSince,omitempty"`
		IfUnmodifiedSince *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfUnmodifiedSince,omitempty"`
		Timestamp         *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.IfModifiedSince = (*xsdDateTime)(&layout.T.IfModifiedSince)
	layout.IfUnmodifiedSince = (*xsdDateTime)(&layout.T.IfUnmodifiedSince)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *GetObjectExtended) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T GetObjectExtended
	var overlay struct {
		*T
		IfModifiedSince   *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfModifiedSince,omitempty"`
		IfUnmodifiedSince *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IfUnmodifiedSince,omitempty"`
		Timestamp         *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.IfModifiedSince = (*xsdDateTime)(&overlay.T.IfModifiedSince)
	overlay.IfUnmodifiedSince = (*xsdDateTime)(&overlay.T.IfUnmodifiedSince)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type GetObjectExtendedResponse struct {
	GetObjectResponse GetObjectResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetObjectResponse"`
}

type GetObjectResponse struct {
	GetObjectResponse GetObjectResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetObjectResponse"`
}

type GetObjectResult struct {
	Metadata     []MetadataEntry `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Metadata,omitempty"`
	Data         []byte          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Data,omitempty"`
	LastModified time.Time       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	ETag         string          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ETag"`
	Status       Status          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Status"`
}

func (t *GetObjectResult) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T GetObjectResult
	var layout struct {
		*T
		Data         *xsdBase64Binary `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Data,omitempty"`
		LastModified *xsdDateTime     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	layout.T = (*T)(t)
	layout.Data = (*xsdBase64Binary)(&layout.T.Data)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *GetObjectResult) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T GetObjectResult
	var overlay struct {
		*T
		Data         *xsdBase64Binary `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Data,omitempty"`
		LastModified *xsdDateTime     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.Data = (*xsdBase64Binary)(&overlay.T.Data)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

type Grant struct {
	Grantee    Grantee    `xml:"Grantee"`
	Permission Permission `xml:"Permission"`
}

type Grantee struct {
	XMLNS       string `xml:"xmlns:xsi,attr"`
	XMLXSI      string `xml:"xsi:type,attr"`
	Type        string `xml:"Type"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

type Group struct {
	URI string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ URI"`
}

type ListAllMyBuckets struct {
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
}

func (t *ListAllMyBuckets) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T ListAllMyBuckets
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *ListAllMyBuckets) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T ListAllMyBuckets
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type ListAllMyBucketsEntry struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

func (t *ListAllMyBucketsEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T ListAllMyBucketsEntry
	var layout struct {
		*T
		CreationDate *xsdDateTime `xml:"CreationDate"`
	}
	layout.T = (*T)(t)
	layout.CreationDate = (*xsdDateTime)(&layout.T.CreationDate)
	return e.EncodeElement(layout, start)
}
func (t *ListAllMyBucketsEntry) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T ListAllMyBucketsEntry
	var overlay struct {
		*T
		CreationDate *xsdDateTime `xml:"CreationDate"`
	}
	overlay.T = (*T)(t)
	overlay.CreationDate = (*xsdDateTime)(&overlay.T.CreationDate)
	return d.DecodeElement(&overlay, &start)
}

type ListAllMyBucketsList struct {
	Bucket []ListAllMyBucketsEntry `xml:"Bucket,omitempty"`
}

type ListAllMyBucketsResponse struct {
	ListAllMyBucketsResponse ListAllMyBucketsResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResponse"`
}

type ListBucket struct {
	Bucket         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Prefix         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Prefix,omitempty"`
	Marker         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Marker,omitempty"`
	MaxKeys        int       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ MaxKeys,omitempty"`
	Delimiter      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Delimiter,omitempty"`
	AWSAccessKeyId string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp      time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature      string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential     string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *ListBucket) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T ListBucket
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *ListBucket) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T ListBucket
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type ListBucketResponse struct {
	ListBucketResponse ListBucketResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResponse"`
}

type ListBucketResult struct {
	XMLName        xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Metadata       []MetadataEntry `xml:"Metadata,omitempty"`
	Name           string          `xml:"Name"`
	Prefix         string          `xml:"Prefix"`
	Marker         string          `xml:"Marker"`
	NextMarker     string          `xml:"NextMarker,omitempty"`
	MaxKeys        int             `xml:"MaxKeys"`
	Delimiter      string          `xml:"Delimiter,omitempty"`
	IsTruncated    bool            `xml:"IsTruncated"`
	Contents       []ListEntry     `xml:"Contents,omitempty"`
	CommonPrefixes []PrefixEntry   `xml:"CommonPrefixes,omitempty"`
}

type ListEntry struct {
	Key          string        `xml:"Key"`
	LastModified time.Time     `xml:"LastModified"`
	ETag         string        `xml:"ETag"`
	Size         int64         `xml:"Size"`
	Owner        CanonicalUser `xml:"Owner,omitempty"`
	StorageClass StorageClass  `xml:"StorageClass"`
}

func (t *ListEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T ListEntry
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *ListEntry) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T ListEntry
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

type ListVersionsResponse struct {
	ListVersionsResponse ListVersionsResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResponse"`
}

type ListVersionsResult struct {
	Metadata            []MetadataEntry   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Metadata,omitempty"`
	Name                string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Name"`
	Prefix              string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Prefix"`
	KeyMarker           string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ KeyMarker"`
	VersionIdMarker     string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ VersionIdMarker"`
	NextKeyMarker       string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ NextKeyMarker,omitempty"`
	NextVersionIdMarker string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ NextVersionIdMarker,omitempty"`
	MaxKeys             int               `xml:"http://s3.amazonaws.com/doc/2006-03-01/ MaxKeys"`
	Delimiter           string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Delimiter,omitempty"`
	IsTruncated         bool              `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IsTruncated"`
	Version             VersionEntry      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Version"`
	DeleteMarker        DeleteMarkerEntry `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteMarker"`
	CommonPrefixes      []PrefixEntry     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CommonPrefixes,omitempty"`
}

type LocationConstraint struct {
	LocationConstraint string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
}

type LoggingSettings struct {
	TargetBucket string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ TargetBucket"`
	TargetPrefix string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ TargetPrefix"`
	TargetGrants AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ TargetGrants,omitempty"`
}

// May be one of COPY, REPLACE
type MetadataDirective string

type MetadataEntry struct {
	Name  string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Name"`
	Value string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Value"`
}

// May be one of Enabled, Disabled
type MfaDeleteStatus string

type NotificationConfiguration struct {
	TopicConfiguration []TopicConfiguration `xml:"http://s3.amazonaws.com/doc/2006-03-01/ TopicConfiguration,omitempty"`
}

// May be one of BucketOwner, Requester
type Payer string

// May be one of READ, WRITE, READ_ACP, WRITE_ACP, FULL_CONTROL
type Permission string

type PostResponse struct {
	Location string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Location"`
	Bucket   string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key      string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	ETag     string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ETag"`
}

type PrefixEntry struct {
	Prefix string `xml:"Prefix"`
}

type PutObject struct {
	Bucket            string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key               string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	Metadata          []MetadataEntry   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Metadata,omitempty"`
	ContentLength     int64             `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ContentLength"`
	AccessControlList AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlList,omitempty"`
	StorageClass      StorageClass      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ StorageClass,omitempty"`
	AWSAccessKeyId    string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp         time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature         string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential        string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *PutObject) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T PutObject
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *PutObject) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T PutObject
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type PutObjectInline struct {
	Bucket            string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key               string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	Metadata          []MetadataEntry   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Metadata,omitempty"`
	Data              []byte            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Data"`
	ContentLength     int64             `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ContentLength"`
	AccessControlList AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlList,omitempty"`
	StorageClass      StorageClass      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ StorageClass,omitempty"`
	AWSAccessKeyId    string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp         time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature         string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential        string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *PutObjectInline) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T PutObjectInline
	var layout struct {
		*T
		Data      *xsdBase64Binary `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Data"`
		Timestamp *xsdDateTime     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Data = (*xsdBase64Binary)(&layout.T.Data)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *PutObjectInline) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T PutObjectInline
	var overlay struct {
		*T
		Data      *xsdBase64Binary `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Data"`
		Timestamp *xsdDateTime     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Data = (*xsdBase64Binary)(&overlay.T.Data)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type PutObjectInlineResponse struct {
	PutObjectInlineResponse PutObjectResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PutObjectInlineResponse"`
}

type PutObjectResponse struct {
	PutObjectResponse PutObjectResult `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PutObjectResponse"`
}

type PutObjectResult struct {
	ETag         string    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ETag"`
	LastModified time.Time `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
}

func (t *PutObjectResult) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T PutObjectResult
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *PutObjectResult) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T PutObjectResult
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

type RequestPaymentConfiguration struct {
	Payer Payer `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Payer"`
}

type Result struct {
	Status Status `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Status"`
}

type SetBucketAccessControlPolicy struct {
	Bucket            string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	AccessControlList AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlList,omitempty"`
	AWSAccessKeyId    string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp         time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature         string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential        string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *SetBucketAccessControlPolicy) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T SetBucketAccessControlPolicy
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *SetBucketAccessControlPolicy) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T SetBucketAccessControlPolicy
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type SetBucketAccessControlPolicyResponse struct {
}

type SetBucketLoggingStatus struct {
	Bucket              string              `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	AWSAccessKeyId      string              `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp           time.Time           `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature           string              `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential          string              `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
	BucketLoggingStatus BucketLoggingStatus `xml:"http://s3.amazonaws.com/doc/2006-03-01/ BucketLoggingStatus"`
}

func (t *SetBucketLoggingStatus) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T SetBucketLoggingStatus
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *SetBucketLoggingStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T SetBucketLoggingStatus
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type SetBucketLoggingStatusResponse struct {
}

type SetObjectAccessControlPolicy struct {
	Bucket            string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Bucket"`
	Key               string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	AccessControlList AccessControlList `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlList"`
	AWSAccessKeyId    string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AWSAccessKeyId,omitempty"`
	Timestamp         time.Time         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	Signature         string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Signature,omitempty"`
	Credential        string            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Credential,omitempty"`
}

func (t *SetObjectAccessControlPolicy) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T SetObjectAccessControlPolicy
	var layout struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	layout.T = (*T)(t)
	layout.Timestamp = (*xsdDateTime)(&layout.T.Timestamp)
	return e.EncodeElement(layout, start)
}
func (t *SetObjectAccessControlPolicy) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T SetObjectAccessControlPolicy
	var overlay struct {
		*T
		Timestamp *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Timestamp,omitempty"`
	}
	overlay.T = (*T)(t)
	overlay.Timestamp = (*xsdDateTime)(&overlay.T.Timestamp)
	return d.DecodeElement(&overlay, &start)
}

type SetObjectAccessControlPolicyResponse struct {
}

type Status struct {
	Code        int    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Code"`
	Description string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Description"`
}

// May be one of STANDARD, REDUCED_REDUNDANCY, GLACIER, UNKNOWN
type StorageClass string

type TopicConfiguration struct {
	Topic string   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Topic"`
	Event []string `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Event"`
}

type User struct {
}

type VersionEntry struct {
	Key          string        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Key"`
	VersionId    string        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ VersionId"`
	IsLatest     bool          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ IsLatest"`
	LastModified time.Time     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	ETag         string        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ETag"`
	Size         int64         `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Size"`
	Owner        CanonicalUser `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Owner,omitempty"`
	StorageClass StorageClass  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ StorageClass"`
}

func (t *VersionEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T VersionEntry
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *VersionEntry) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T VersionEntry
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

type VersioningConfiguration struct {
	Status    VersioningStatus `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Status,omitempty"`
	MfaDelete MfaDeleteStatus  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ MfaDelete,omitempty"`
}

// May be one of Enabled, Suspended
type VersioningStatus string

type xsdBase64Binary []byte

func (b *xsdBase64Binary) UnmarshalText(text []byte) (err error) {
	*b, err = base64.StdEncoding.DecodeString(string(text))
	return
}
func (b xsdBase64Binary) MarshalText() ([]byte, error) {
	var buf bytes.Buffer
	enc := base64.NewEncoder(base64.StdEncoding, &buf)
	enc.Write([]byte(b))
	enc.Close()
	return buf.Bytes(), nil
}

type xsdDateTime time.Time

func (t *xsdDateTime) UnmarshalText(text []byte) error {
	return _unmarshalTime(text, (*time.Time)(t), s3TimeFormat)
}
func (t xsdDateTime) MarshalText() ([]byte, error) {
	return []byte((time.Time)(t).Format(s3TimeFormat)), nil
}
func (t xsdDateTime) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if (time.Time)(t).IsZero() {
		return nil
	}
	m, err := t.MarshalText()
	if err != nil {
		return err
	}
	return e.EncodeElement(m, start)
}
func (t xsdDateTime) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	if (time.Time)(t).IsZero() {
		return xml.Attr{}, nil
	}
	m, err := t.MarshalText()
	return xml.Attr{Name: name, Value: string(m)}, err
}
func _unmarshalTime(text []byte, t *time.Time, format string) (err error) {
	s := string(bytes.TrimSpace(text))
	*t, err = time.Parse(format, s)
	if _, ok := err.(*time.ParseError); ok {
		*t, err = time.Parse(format+"Z07:00", s)
	}
	return err
}
