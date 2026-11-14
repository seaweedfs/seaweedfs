package shell

import (
	"bytes"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Case struct {
	args   []string
	result string
}

var (
	TestCases = []*Case{
		//add circuit breaker config for global
		{
			args: strings.Split("-global -type Count -actions Read,Write -values 500,200", " "),
			result: `{
				"global": {
					"enabled": true,
					"actions": {
						"Read:Count": "500",
						"Write:Count": "200"
					}
				}
			}`,
		},

		//disable global config
		{
			args: strings.Split("-global -disable", " "),
			result: `{
				"global": {
					"actions": {
						"Read:Count": "500",
						"Write:Count": "200"
					}
				}
			}`,
		},

		//add circuit breaker config for buckets x,y,z
		{
			args: strings.Split("-buckets x,y,z -type Count -actions Read,Write -values 200,100", " "),
			result: `{
				"global": {
					"actions": {
						"Read:Count": "500",
						"Write:Count": "200"
					}
				},
				"buckets": {
					"x": {
						"enabled": true,
						"actions": {
							"Read:Count": "200",
							"Write:Count": "100"
						}
					},
					"y": {
						"enabled": true,
						"actions": {
							"Read:Count": "200",
							"Write:Count": "100"
						}
					},
					"z": {
						"enabled": true,
						"actions": {
							"Read:Count": "200",
							"Write:Count": "100"
						}
					}
				}
			}`,
		},

		//disable circuit breaker config of x
		{
			args: strings.Split("-buckets x -disable", " "),
			result: `{
			  "global": {
				"actions": {
				  "Read:Count": "500",
				  "Write:Count": "200"
				}
			  },
			  "buckets": {
				"x": {
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				},
				"y": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				}
			  }
			}`,
		},

		//delete circuit breaker config of x
		{
			args: strings.Split("-buckets x -delete", " "),
			result: `{
			  "global": {
				"actions": {
				  "Read:Count": "500",
				  "Write:Count": "200"
				}
			  },
			  "buckets": {
				"y": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				}
			  }
			}`,
		},

		//configure the circuit breaker for the size of the uploaded file for bucket x,y
		{
			args: strings.Split("-buckets x,y -type MB -actions Write -values 1024", " "),
			result: `{
			  "global": {
				"actions": {
				  "Read:Count": "500",
				  "Write:Count": "200"
				}
			  },
			  "buckets": {
				"x": {
				  "enabled": true,
				  "actions": {
					"Write:MB": "1073741824"
				  }
				},
				"y": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100",
					"Write:MB": "1073741824"
				  }
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				}
			  }
			}`,
		},

		//delete the circuit breaker configuration for the size of the uploaded file of bucket x,y
		{
			args: strings.Split("-buckets x,y -type MB -actions Write -delete", " "),
			result: `{
			  "global": {
				"actions": {
				  "Read:Count": "500",
				  "Write:Count": "200"
				}
			  },
			  "buckets": {
				"x": {
				  "enabled": true
				},
				"y": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				}
			  }
			}`,
		},

		//enable global circuit breaker config (without -disable flag)
		{
			args: strings.Split("-global", " "),
			result: `{
			  "global": {
				"enabled": true,
				"actions": {
				  "Read:Count": "500",
				  "Write:Count": "200"
				}
			  },
			  "buckets": {
				"x": {
				  "enabled": true
				},
				"y": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100"
				  }
				}
			  }
			}`,
		},

		//clear all circuit breaker config
		{
			args: strings.Split("-delete", " "),
			result: `{
			
			}`,
		},
	}
)

func TestCircuitBreakerShell(t *testing.T) {
	var writeBuf bytes.Buffer
	cmd := &commandS3CircuitBreaker{}
	originalLoadConfig := LoadConfig
	t.Cleanup(func() { LoadConfig = originalLoadConfig })
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

		// Compare as configs: the command writes default fields such as
		// "enabled": false that the expected JSON leaves out.
		actual := &s3_pb.S3CircuitBreakerConfig{}
		if err := filer.ParseS3ConfigurationFromBytes(writeBuf.Bytes(), actual); err != nil {
			t.Fatalf("case %d %v: parse output: %v", i, tc.args, err)
		}
		// Strict parsing, so a misspelled field in the expected JSON fails.
		expect := &s3_pb.S3CircuitBreakerConfig{}
		if err := protojson.Unmarshal([]byte(tc.result), expect); err != nil {
			t.Fatalf("case %d: parse expected result: %v", i, err)
		}
		if !proto.Equal(actual, expect) {
			t.Fatalf("case %d %v:\ngot  %v\nwant %v", i, tc.args, actual, expect)
		}
	}
}
