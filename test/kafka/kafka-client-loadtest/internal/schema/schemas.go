package schema

// GetAvroSchema returns the Avro schema for load test messages
func GetAvroSchema() string {
	return `{
		"type": "record",
		"name": "LoadTestMessage",
		"namespace": "com.seaweedfs.loadtest",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "producer_id", "type": "int"},
			{"name": "counter", "type": "long"},
			{"name": "user_id", "type": "string"},
			{"name": "event_type", "type": "string"},
			{"name": "properties", "type": {"type": "map", "values": "string"}}
		]
	}`
}

// GetJSONSchema returns the JSON Schema for load test messages
func GetJSONSchema() string {
	return `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "LoadTestMessage",
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"timestamp": {"type": "integer"},
			"producer_id": {"type": "integer"},
			"counter": {"type": "integer"},
			"user_id": {"type": "string"},
			"event_type": {"type": "string"},
			"properties": {
				"type": "object",
				"additionalProperties": {"type": "string"}
			}
		},
		"required": ["id", "timestamp", "producer_id", "counter", "user_id", "event_type"]
	}`
}

// GetProtobufSchema returns the Protobuf schema for load test messages
func GetProtobufSchema() string {
	return `syntax = "proto3";

package com.seaweedfs.loadtest;

message LoadTestMessage {
  string id = 1;
  int64 timestamp = 2;
  int32 producer_id = 3;
  int64 counter = 4;
  string user_id = 5;
  string event_type = 6;
  map<string, string> properties = 7;
}`
}
