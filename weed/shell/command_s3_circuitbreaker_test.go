package shell

import (
	"bytes"
	"strings"
	"testing"
)

type Case struct {
	args   []string
	result string
}

var (
	TestCases = []*Case{
		//add circuit breaker config for global
		{
			args:   strings.Split("-global -type count -actions Read,Write -values 500,200", " "),
			result: "{\n  \"global\": {\n    \"enabled\": true,\n    \"actions\": {\n      \"Read:count\": \"500\",\n      \"Write:count\": \"200\"\n    }\n  }\n}\n",
		},
		//disable global config
		{
			args:   strings.Split("-global -disable", " "),
			result: "{\n  \"global\": {\n    \"actions\": {\n      \"Read:count\": \"500\",\n      \"Write:count\": \"200\"\n    }\n  }\n}\n",
		},
		//add circuit breaker config for buckets x,y,z
		{
			args:   strings.Split("-buckets x,y,z -type count -actions Read,Write -values 200,100", " "),
			result: "{\n  \"global\": {\n    \"actions\": {\n      \"Read:count\": \"500\",\n      \"Write:count\": \"200\"\n    }\n  },\n  \"buckets\": {\n    \"x\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    },\n    \"y\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    },\n    \"z\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    }\n  }\n}\n",
		},
		//disable circuit breaker config of x
		{
			args:   strings.Split("-buckets x -disable", " "),
			result: "{\n  \"global\": {\n    \"actions\": {\n      \"Read:count\": \"500\",\n      \"Write:count\": \"200\"\n    }\n  },\n  \"buckets\": {\n    \"x\": {\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    },\n    \"y\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    },\n    \"z\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    }\n  }\n}\n",
		},
		//delete circuit breaker config of x
		{
			args:   strings.Split("-buckets x -delete", " "),
			result: "{\n  \"global\": {\n    \"actions\": {\n      \"Read:count\": \"500\",\n      \"Write:count\": \"200\"\n    }\n  },\n  \"buckets\": {\n    \"y\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    },\n    \"z\": {\n      \"enabled\": true,\n      \"actions\": {\n        \"Read:count\": \"200\",\n        \"Write:count\": \"100\"\n      }\n    }\n  }\n}\n",
		},
		//clear all circuit breaker config
		{
			args:   strings.Split("-delete", " "),
			result: "{\n\n}\n",
		},
	}
)

func TestCircuitBreakerShell(t *testing.T) {
	var writeBuf bytes.Buffer
	cmd := &commandS3CircuitBreaker{}
	LoadConfig = func(commandEnv *CommandEnv, dir string, file string, buf *bytes.Buffer) error {
		_, err := buf.Write(writeBuf.Bytes())
		if err != nil {
			return err
		}
		writeBuf.Reset()
		return nil
	}

	for i, tc := range TestCases {
		err := cmd.Do(tc.args, nil, &writeBuf)
		if err != nil {
			t.Fatal(err)
		}
		if i != 0 {
			result := writeBuf.String()
			if result != tc.result {
				t.Fatal("result of s3 circuit breaker shell command is unexpect!")
			}

		}
	}
}
