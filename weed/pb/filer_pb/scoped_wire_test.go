package filer_pb

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Captured in Docker from upstream 4.51's generated types with the original
// fork's 0007 generated-file patch applied. Never derive this from current types.
//
//go:embed testdata/scoped_wire_451.json
var legacyScopedWire []byte

func TestFilerChunkProofWireRoundTrip(t *testing.T) {
	var fixture struct {
		Unknown string `json:"unknown_hex"`
		Cases   []struct {
			Message     string `json:"message"`
			Wire        string `json:"wire_hex"`
			WithUnknown string `json:"with_unknown_hex"`
			Fields      []struct {
				Name   string `json:"name"`
				Number int32  `json:"number"`
				Kind   string `json:"kind"`
				Map    bool   `json:"map"`
			} `json:"fields"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(legacyScopedWire, &fixture); err != nil {
		t.Fatal(err)
	}
	unknown, err := hex.DecodeString(fixture.Unknown)
	if err != nil || len(unknown) == 0 {
		t.Fatalf("invalid legacy unknown-field fixture: %v", err)
	}
	const fid = "3,00010000000001"
	proof := map[string]string{fid: "fixture-proof"}
	entry := &Entry{Name: "legacy.txt", HardLinkId: []byte{1, 2, 3},
		Chunks: []*FileChunk{{FileId: fid, Offset: 7, Size: 11}}}
	wants := map[string]proto.Message{
		"CreateEntryRequest":   &CreateEntryRequest{Directory: "/buckets/a", Entry: entry, ChunkAuth: proof, LinkSource: "/buckets/a/source"},
		"UpdateEntryRequest":   &UpdateEntryRequest{Directory: "/buckets/a", Entry: entry, ChunkAuth: proof},
		"AppendToEntryRequest": &AppendToEntryRequest{Directory: "/buckets/a", EntryName: "legacy.txt", Chunks: entry.Chunks, ChunkAuth: proof},
		"AssignVolumeResponse": &AssignVolumeResponse{FileId: fid, Count: 1, AssignAuth: "fixture-assignment"},
	}
	if len(fixture.Cases) != len(wants) {
		t.Fatalf("legacy cases = %d, want %d", len(fixture.Cases), len(wants))
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Message, func(t *testing.T) {
			want, okay := wants[tc.Message]
			if !okay {
				t.Fatalf("unknown legacy case %q", tc.Message)
			}
			wire, err := hex.DecodeString(tc.Wire)
			if err != nil {
				t.Fatal(err)
			}
			withUnknown, err := hex.DecodeString(tc.WithUnknown)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := want.ProtoReflect().Descriptor()
			for _, old := range tc.Fields {
				field := descriptor.Fields().ByNumber(protoreflect.FieldNumber(old.Number))
				if field == nil || string(field.Name()) != old.Name || field.Kind().String() != old.Kind || field.IsMap() != old.Map {
					t.Fatalf("legacy field %s=%d changed or number reused", old.Name, old.Number)
				}
			}
			if descriptor.Fields().ByNumber(50000) != nil || descriptor.Fields().ByNumber(50001) != nil {
				t.Fatal("unknown-field fixture numbers are no longer unknown")
			}
			decoded := want.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(wire, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, want) {
				t.Fatalf("legacy encoder -> new decoder lost fields: %v", decoded)
			}
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, wire) {
				t.Fatal("new encoder differs from bytes accepted by original fork decoder")
			}
			if err := proto.Unmarshal(withUnknown, decoded); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded.ProtoReflect().GetUnknown(), unknown) {
				t.Fatal("protobuf decoder lost unknown legacy fields")
			}
			encoded, err = proto.MarshalOptions{Deterministic: true}.Marshal(decoded)
			if err != nil || !bytes.Equal(encoded, withUnknown) {
				t.Fatalf("protobuf relay lost known/unknown fields: %v", err)
			}
			vt := want.ProtoReflect().New().Interface()
			codec, okay := vt.(interface {
				UnmarshalVT([]byte) error
				MarshalVT() ([]byte, error)
			})
			if !okay {
				t.Fatal("generated VT codec is missing")
			}
			if err := codec.UnmarshalVT(withUnknown); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(vt, decoded) {
				t.Fatal("VT decoder differs from protobuf decoder")
			}
			encoded, err = codec.MarshalVT()
			if err != nil {
				t.Fatal(err)
			}
			relayed := want.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(encoded, relayed); err != nil || !proto.Equal(relayed, decoded) {
				t.Fatalf("VT relay lost known/unknown fields: %v", err)
			}
			if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(withUnknown, relayed); err != nil {
				t.Fatal(err)
			}
			if len(relayed.ProtoReflect().GetUnknown()) != 0 || !proto.Equal(relayed, want) {
				t.Fatal("discarding unknown fields changed the understood legacy payload")
			}
		})
	}
}
