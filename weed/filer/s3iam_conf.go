package filer

import (
	"bytes"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"io"
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
