package filer

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func ParseS3ConfigurationFromBytes[T proto.Message](content []byte, config T) error {
	options := &jsonpb.UnmarshalOptions{
		DiscardUnknown: true,
		AllowPartial:   true,
	}
	if err := options.Unmarshal(content, config); err != nil {
		return err
	}
	return nil
}

func ProtoToText(writer io.Writer, config proto.Message) error {

	m := jsonpb.MarshalOptions{
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(config)
	if marshalErr != nil {
		return fmt.Errorf("marshal proto message: %v", marshalErr)
	}

	_, writeErr := writer.Write(text)
	if writeErr != nil {
		return fmt.Errorf("fail to write proto message: %v", writeErr)
	}

	return writeErr
}

// CheckDuplicateAccessKey returns an error message when s3cfg has duplicate access keys
func CheckDuplicateAccessKey(s3cfg *iam_pb.S3ApiConfiguration) error {
	accessKeySet := make(map[string]string)
	for _, ident := range s3cfg.Identities {
		for _, cred := range ident.Credentials {
			if userName, found := accessKeySet[cred.AccessKey]; !found {
				accessKeySet[cred.AccessKey] = ident.Name
			} else if userName != ident.Name {
				return fmt.Errorf("duplicate accessKey[%s], already configured in user[%s]", cred.AccessKey, userName)
			}
		}
	}
	return nil
}
