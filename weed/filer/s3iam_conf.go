package filer

import (
	"bytes"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"io"
)

func ParseS3ConfigurationFromBytes(content []byte, config *iam_pb.S3ApiConfiguration) error {
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
