package s3api

import (
	"encoding/xml"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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

type TestTags map[string]string

var ValidateTagsTestCases = []struct {
	testCaseID    int
	tags          TestTags
	wantErrString string
}{
	{
		1,
		TestTags{"key-1": "value-1"},
		"",
	},
	{
		2,
		TestTags{"key-1": "valueOver256R59YI9bahPwAVqvLeKCvM2S1RjzgP8fNDKluCbol0XTTFY6VcMwTBmdnqjsddilXztSGfEoZS1wDAIMBA0rW0CLNSoE2zNg4TT0vDbLHEtZBoZjdZ5E0JNIAqwb9ptIk2VizYmhWjb1G4rJ0CqDGWxcy3usXaQg6Dk6kU8N4hlqwYWeGw7uqdghcQ3ScfF02nHW9QFMN7msLR5fe90mbFBBp3Tjq34i0LEr4By2vxoRa2RqdBhEJhi23Tm"},
		"validate tags: tag value longer than 256",
	},
	{
		3,
		TestTags{"keyLenOver128a5aUUGcPexMELsz3RyROzIzfO6BKABeApH2nbbagpOxZh2MgBWYDZtFxQaCuQeP1xR7dUJLwfFfDHguVIyxvTStGDk51BemKETIwZ0zkhR7lhfHBp2y0nFnV": "value-1"},
		"validate tags: tag key longer than 128",
	},
	{
		4,
		TestTags{"key-1*": "value-1"},
		"validate tags key key-1* error, incorrect key",
	},
	{
		5,
		TestTags{"key-1": "value-1?"},
		"validate tags value value-1? error, incorrect value",
	},
	{
		6,
		TestTags{
			"key-1":  "value",
			"key-2":  "value",
			"key-3":  "value",
			"key-4":  "value",
			"key-5":  "value",
			"key-6":  "value",
			"key-7":  "value",
			"key-8":  "value",
			"key-9":  "value",
			"key-10": "value",
			"key-11": "value",
		},
		"validate tags: 11 tags more than 10",
	},
}

func TestValidateTags(t *testing.T) {
	for _, testCase := range ValidateTagsTestCases {
		err := ValidateTags(testCase.tags)
		if testCase.wantErrString == "" {
			assert.NoErrorf(t, err, "no error")
		} else {
			assert.EqualError(t, err, testCase.wantErrString)
		}
	}
}
