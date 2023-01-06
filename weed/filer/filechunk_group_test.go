package filer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestChunkGroup_doSearchChunks(t *testing.T) {
	type fields struct {
		sections map[SectionIndex]*FileChunkSection
	}
	type args struct {
		offset   int64
		fileSize int64
		whence   uint32
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantFound bool
		wantOut   int64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group := &ChunkGroup{
				sections: tt.fields.sections,
			}
			gotFound, gotOut := group.doSearchChunks(tt.args.offset, tt.args.fileSize, tt.args.whence)
			assert.Equalf(t, tt.wantFound, gotFound, "doSearchChunks(%v, %v, %v)", tt.args.offset, tt.args.fileSize, tt.args.whence)
			assert.Equalf(t, tt.wantOut, gotOut, "doSearchChunks(%v, %v, %v)", tt.args.offset, tt.args.fileSize, tt.args.whence)
		})
	}
}
