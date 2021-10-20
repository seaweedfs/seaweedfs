package s3api

import (
	"encoding/xml"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestXMLUnmarshall(t *testing.T) {

	input := `<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <TagSet>
      <Tag>
         <Key>key1</Key>
         <Value>value1</Value>
      </Tag>
   </TagSet>
</Tagging>
`

	tags := &Tagging{}

	xml.Unmarshal([]byte(input), tags)

	assert.Equal(t, len(tags.TagSet.Tag), 1)
	assert.Equal(t, tags.TagSet.Tag[0].Key, "key1")
	assert.Equal(t, tags.TagSet.Tag[0].Value, "value1")

}

func TestXMLMarshall(t *testing.T) {
	tags := &Tagging{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet: TagSet{
			[]Tag{
				{
					Key:   "key1",
					Value: "value1",
				},
			},
		},
	}

	actual := string(s3err.EncodeXMLResponse(tags))

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>key1</Key><Value>value1</Value></Tag></TagSet></Tagging>`
	assert.Equal(t, expected, actual)

}
