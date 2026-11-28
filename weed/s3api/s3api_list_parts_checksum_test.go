package s3api

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func checksumPartEntry(name string, size uint64, headerName, value string) *filer_pb.Entry {
	entry := partEntry(name, size)
	entry.Extended = map[string][]byte{
		s3_constants.ExtChecksumAlgorithm: []byte(headerName),
		s3_constants.ExtChecksumValue:     []byte(value),
	}
	return entry
}

func TestListPartsReturnsPartChecksums(t *testing.T) {
	const sha256Value = "Jf0Q2NhRDXXxtFj5iYBZnDXcrZ0e9U/hkc+cu7XJNkI="
	const crc64Value = "AAAAAAAAAAA="
	s3a := newListPartsServer(t, &fakePartsFiler{
		uploadEntry: uploadRecordEntry("checksum-upload"),
		parts: []*filer_pb.Entry{
			checksumPartEntry("0001.part", 5, s3_constants.AmzChecksumSHA256, sha256Value),
			checksumPartEntry("0002.part", 5, s3_constants.AmzChecksumCRC64NVME, crc64Value),
			partEntry("0003.part", 5),
		},
	})

	output, code := s3a.listObjectParts(listPartsInput("checksum-upload"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if len(output.Part) != 3 {
		t.Fatalf("parts = %d, want 3", len(output.Part))
	}
	if got := output.Part[0].ChecksumSHA256; got != sha256Value {
		t.Fatalf("part 1 ChecksumSHA256 = %q, want %q", got, sha256Value)
	}
	if got := output.Part[1].ChecksumCRC64NVME; got != crc64Value {
		t.Fatalf("part 2 ChecksumCRC64NVME = %q, want %q", got, crc64Value)
	}
	if output.Part[2].ChecksumResult != (ChecksumResult{}) {
		t.Fatalf("part 3 checksums = %+v, want none", output.Part[2].ChecksumResult)
	}

	body := string(s3err.EncodeXMLBody(output))
	for _, want := range []string{
		"<Part><ChecksumSHA256>" + sha256Value + "</ChecksumSHA256><ETag>",
		"<Part><ChecksumCRC64NVME>" + crc64Value + "</ChecksumCRC64NVME><ETag>",
		"<PartNumber>3</PartNumber>",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("ListParts body missing %q:\n%s", want, body)
		}
	}
	if strings.Count(body, "<Checksum") != 2 {
		t.Fatalf("ListParts body has checksum elements for an unchecksummed part:\n%s", body)
	}
}

func TestListPartsReturnsUploadChecksumAlgorithm(t *testing.T) {
	upload := uploadRecordEntry("checksum-upload")
	upload.Extended[s3_constants.ExtChecksumAlgorithm] = []byte(s3_constants.AmzChecksumSHA256)
	upload.Extended[s3_constants.ExtChecksumType] = []byte(s3_constants.ChecksumTypeComposite)
	s3a := newListPartsServer(t, &fakePartsFiler{uploadEntry: upload})

	output, code := s3a.listObjectParts(listPartsInput("checksum-upload"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if output.ChecksumAlgorithm != "SHA256" || output.ChecksumType != s3_constants.ChecksumTypeComposite {
		t.Fatalf("checksum algorithm/type = %q/%q, want SHA256/%s", output.ChecksumAlgorithm, output.ChecksumType, s3_constants.ChecksumTypeComposite)
	}
	body := string(s3err.EncodeXMLBody(output))
	if !strings.Contains(body, "<ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType>") {
		t.Fatalf("ListParts body missing the upload checksum algorithm:\n%s", body)
	}

	plain := newListPartsServer(t, &fakePartsFiler{uploadEntry: uploadRecordEntry("plain-upload")})
	output, code = plain.listObjectParts(listPartsInput("plain-upload"))
	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if body := string(s3err.EncodeXMLBody(output)); strings.Contains(body, "<ChecksumAlgorithm>") || strings.Contains(body, "<ChecksumType>") {
		t.Fatalf("ListParts body names a checksum for an upload created without one:\n%s", body)
	}
}
