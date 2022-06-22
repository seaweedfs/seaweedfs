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
			args: strings.Split("-buckets x,y -type MB -actions Write -values 10", " "),
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
					"Write:MB": "10"
				  }
				},
				"y": {
				  "enabled": true,
				  "actions": {
					"Read:Count": "200",
					"Write:Count": "100",
					"Write:MB": "10"
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

		//delete buckets-Write-MB config of bucket x,y
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

		//delete global-Write-Count config
		{
			args: strings.Split("-global -actions Write -type Count -delete", " "),
			result: `{
			  "global": {
				"enabled": true,
				"actions": {
				  "Read:Count": "500"
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

		//delete global-read (without -type specified will delete both 'Count' and 'MB' config)
		{
			args: strings.Split("-global -actions Read -delete", " "),
			result: `{
			  "global": {
				"enabled": true
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

		//delete all 'Count' type config (both global and buckets)
		{
			args: strings.Split("-type Count -delete", " "),
			result: `
				{
				  "global": {
					"enabled": true
				  },
				  "buckets": {
					"x": {
					  "enabled": true
					},
					"y": {
					  "enabled": true
					},
					"z": {
					  "enabled": true
					}
				  }
				}`,
		},

		//delete config of bucket x
		{
			args: strings.Split("-buckets x -delete", " "),
			result: `
				{
				  "global": {
					"enabled": true
				  },
				  "buckets": {
					"y": {
					  "enabled": true
					},
					"z": {
					  "enabled": true
					}
				  }
				}`,
		},

		//add 'Count' config for global (without specify -actions will add all allowed actions by default)
		{
			args: strings.Split("-global -type Count -values 50", " "),
			result: `
				{
				  "global": {
					"enabled": true,
					"actions": {
					  "Admin:Count": "50",
					  "List:Count": "50",
					  "Read:Count": "50",
					  "Tagging:Count": "50",
					  "Write:Count": "50"
					}
				  },
				  "buckets": {
					"y": {
					  "enabled": true
					},
					"z": {
					  "enabled": true
					}
				  }
				}`,
		},

		//add 'MB' config for global (without specify -actions will add all allowed actions by default)
		{
			args: strings.Split("-global -type MB -values 10", " "),
			result: `
				{
				  "global": {
					"enabled": true,
					"actions": {
					  "Admin:MB": "10",
					  "Admin:Count": "50",
					  "List:MB": "10",
					  "List:Count": "50",
					  "Read:MB": "10",
					  "Read:Count": "50",
					  "Tagging:MB": "10",
					  "Tagging:Count": "50",
					  "Write:MB": "10",
					  "Write:Count": "50"
					}
				  },
				  "buckets": {
					"y": {
					  "enabled": true
					},
					"z": {
					  "enabled": true
					}
				  }
				}`,
		},

		//add 'MB' config for bucket z (without specify -actions will add all allowed actions by default)
		{
			args: strings.Split("-buckets z -type MB -values 10", " "),
			result: `{
			  "global": {
				"enabled": true,
				"actions": {
				  "Admin:MB": "10",
				  "Admin:Count": "50",
				  "List:MB": "10",
				  "List:Count": "50",
				  "Read:MB": "10",
				  "Read:Count": "50",
				  "Tagging:MB": "10",
				  "Tagging:Count": "50",
				  "Write:MB": "10",
				  "Write:Count": "50"
				}
			  },
			  "buckets": {
				"y": {
				  "enabled": true
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Admin:MB": "10",
					"List:MB": "10",
					"Read:MB": "10",
					"Tagging:MB": "10",
					"Write:MB": "10"
				  }
				}
			  }
			}`,
		},

		//delete all 'Count' config (without specify -global, -buckets and -actions will delete all 'Count' config of all actions for both global config and buckets config)
		{
			args: strings.Split("-type MB -delete", " "),
			result: `{
			  "global": {
				"enabled": true,
				"actions": {
				  "Admin:Count": "50",
				  "List:Count": "50",
				  "Read:Count": "50",
				  "Tagging:Count": "50",
				  "Write:Count": "50"
				}
			  },
			  "buckets": {
				"y": {
				  "enabled": true
				},
				"z": {
				  "enabled": true
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
			t.Fatalf("err: %v, case: %v", err, tc)
		}
		if i != 0 {
			result := writeBuf.String()

			actual := make(map[string]interface{})
			err := json.Unmarshal([]byte(result), &actual)
			if err != nil {
				t.Fatalf("err: %v, case: %v", err, tc)
			}

			expect := make(map[string]interface{})
			err = json.Unmarshal([]byte(result), &expect)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(actual, expect) {
				t.Fatalf("result of s3 circuit breaker shell command is unexpect! case: %v", tc)
			}
		}
	}
}
