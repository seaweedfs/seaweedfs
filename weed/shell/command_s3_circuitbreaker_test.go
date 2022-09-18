package shell

import (
	"bytes"
	"encoding/json"
	"reflect"
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

			actual := make(map[string]interface{})
			err := json.Unmarshal([]byte(result), &actual)
			if err != nil {
				t.Error(err)
			}

			expect := make(map[string]interface{})
			err = json.Unmarshal([]byte(result), &expect)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(actual, expect) {
				t.Fatal("result of s3 circuit breaker shell command is unexpected!")
			}
		}
	}
}
