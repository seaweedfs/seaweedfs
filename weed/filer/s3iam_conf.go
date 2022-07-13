package filer

import (
	"bytes"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

func ParseS3ConfigurationFromBytes[T proto.Message](content []byte, config T) error {
	if err := jsonpb.Unmarshal(bytes.NewBuffer(content), config); err != nil {
		return err
	}
	return nil
}

func ProtoToText(writer io.Writer, config proto.Message) error {

	m := jsonpb.Marshaler{
		EmitDefaults: false,
		Indent:       "  ",
	}

	return m.Marshal(writer, config)
}

// CheckDuplicateAccessKey returns an error message when s3cfg has duplicate access keys
func CheckDuplicateAccessKey(s3cfg *iam_pb.S3ApiConfiguration) error {
	accessKeySet := make(map[string]string)
	for _, ident := range s3cfg.Identities {
		for _, cred := range ident.Credentials {
			if userName, found := accessKeySet[cred.AccessKey]; !found {
				accessKeySet[cred.AccessKey] = ident.Name
			} else {
				return fmt.Errorf("duplicate accessKey[%s], already configured in user[%s]", cred.AccessKey, userName)
			}
		}
	}
	return nil
}
