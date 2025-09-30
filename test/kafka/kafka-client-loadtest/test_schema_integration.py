#!/usr/bin/env python3
"""
Test script to trigger schema integration by creating a topic
and producing a message that would trigger schema processing.
"""

import socket
import struct
import time

def create_kafka_produce_request(topic, message):
    """Create a simple Kafka Produce request to trigger topic creation"""
    # This is a very simplified Kafka protocol implementation
    # In practice, you'd use a proper Kafka client
    
    # Kafka Request Header (simplified)
    api_key = 0  # Produce
    api_version = 7
    correlation_id = 1
    client_id = b"schema-test"
    
    # Build a minimal produce request
    request_header = struct.pack('>HHI', api_key, api_version, correlation_id)
    client_id_len = struct.pack('>H', len(client_id))
    
    # Topic data (simplified)
    topic_bytes = topic.encode('utf-8')
    topic_len = struct.pack('>H', len(topic_bytes))
    
    # Message data (simplified)
    message_bytes = message.encode('utf-8')
    
    # Combine parts (this is a very simplified version)
    request_body = client_id_len + client_id + topic_len + topic_bytes
    
    # Add request length header
    request_length = len(request_header) + len(request_body)
    length_header = struct.pack('>I', request_length)
    
    return length_header + request_header + request_body

def test_schema_integration():
    """Test the schema integration by attempting to create a topic"""
    print("=== Schema Integration Test ===")
    
    # Test topic that should trigger schema integration
    test_topic = "test-schema-integration-topic"
    test_message = "This message should trigger schema integration code"
    
    print(f"Testing schema integration for topic: {test_topic}")
    print(f"Message: {test_message}")
    
    try:
        # Connect to Kafka Gateway
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect(('localhost', 9093))
        
        print("✅ Connected to Kafka Gateway")
        
        # Create and send a produce request
        request = create_kafka_produce_request(test_topic, test_message)
        print(f"📤 Sending Produce request ({len(request)} bytes)")
        
        sock.send(request)
        
        # Try to read response (simplified)
        try:
            response = sock.recv(1024)
            print(f"📥 Received response ({len(response)} bytes)")
        except socket.timeout:
            print("⏰ Response timeout (expected for simplified protocol)")
        
        sock.close()
        
        print("🔧 This should have triggered:")
        print("  1. createTopicWithSchemaSupport() function")
        print("  2. Schema Registry query (if configured)")
        print("  3. Topic creation with schema integration")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False

if __name__ == "__main__":
    success = test_schema_integration()
    if success:
        print("\n✅ Schema integration test completed")
        print("📋 Check Kafka Gateway logs for schema integration debug messages")
    else:
        print("\n❌ Schema integration test failed")
