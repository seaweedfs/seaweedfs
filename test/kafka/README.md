# Kafka Gateway Tests with SMQ Integration

This directory contains tests for the SeaweedFS Kafka Gateway with full SeaweedMQ (SMQ) integration.

## Test Types

### **Unit Tests** (`./unit/`)
- Basic gateway functionality
- Protocol compatibility 
- No SeaweedFS backend required
- Uses mock handlers

### **Integration Tests** (`./integration/`)
- **Mock Mode** (default): Uses in-memory handlers for protocol testing
- **SMQ Mode** (with `SEAWEEDFS_MASTERS`): Uses real SeaweedFS backend for full integration

### **E2E Tests** (`./e2e/`)
- End-to-end workflows
- Automatically detects SMQ availability
- Falls back to mock mode if SMQ unavailable

## Running Tests Locally

### Quick Protocol Testing (Mock Mode)
```bash
# Run all integration tests with mock backend
cd test/kafka
go test ./integration/...

# Run specific test
go test -v ./integration/ -run TestClientCompatibility
```

### Full Integration Testing (SMQ Mode)
Requires running SeaweedFS instance:

1. **Start SeaweedFS with MQ support:**
```bash
# Terminal 1: Start SeaweedFS server
weed server -ip="127.0.0.1" -ip.bind="0.0.0.0" -dir=/tmp/seaweedfs-data -master.port=9333 -volume.port=8081 -filer.port=8888 -filer=true

# Terminal 2: Start MQ broker  
weed mq.broker -master="127.0.0.1:9333" -ip="127.0.0.1" -port=17777
```

2. **Run tests with SMQ backend:**
```bash
cd test/kafka
SEAWEEDFS_MASTERS=127.0.0.1:9333 go test ./integration/...

# Run specific SMQ integration tests
SEAWEEDFS_MASTERS=127.0.0.1:9333 go test -v ./integration/ -run TestSMQIntegration
```

### Test Broker Startup
If you're having broker startup issues:
```bash
# Debug broker startup locally
./scripts/test-broker-startup.sh
```

## CI/CD Integration

### GitHub Actions Jobs

1. **Unit Tests** - Fast protocol tests with mock backend
2. **Integration Tests** - Mock mode by default  
3. **E2E Tests (with SMQ)** - Full SeaweedFS + MQ broker stack
4. **Client Compatibility (with SMQ)** - Tests different Kafka clients against real backend
5. **Consumer Group Tests (with SMQ)** - Tests consumer group persistence
6. **SMQ Integration Tests** - Dedicated SMQ-specific functionality tests

### What Gets Tested with SMQ

When `SEAWEEDFS_MASTERS` is available, tests exercise:

- **Real Message Persistence** - Messages stored in SeaweedFS volumes  
- **Offset Persistence** - Consumer group offsets stored in SeaweedFS filer  
- **Topic Persistence** - Topic metadata persisted in SeaweedFS filer  
- **Consumer Group Coordination** - Distributed coordinator assignment  
- **Cross-Client Compatibility** - Sarama, kafka-go with real backend  
- **Broker Discovery** - Gateway discovers MQ brokers via masters  

## Test Infrastructure

### `testutil.NewGatewayTestServerWithSMQ(t, mode)`

Smart gateway creation that automatically:
- Detects SMQ availability via `SEAWEEDFS_MASTERS`
- Uses production handler when available
- Falls back to mock when unavailable  
- Provides timeout protection against hanging

**Modes:**
- `SMQRequired` - Skip test if SMQ unavailable
- `SMQAvailable` - Use SMQ if available, otherwise mock
- `SMQUnavailable` - Always use mock

### Timeout Protection

Gateway creation includes timeout protection to prevent CI hanging:
- 20 second timeout for `SMQRequired` mode
- 15 second timeout for `SMQAvailable` mode  
- Clear error messages when broker discovery fails

## Debugging Failed Tests

### CI Logs to Check
1. **"SeaweedFS master is up"** - Master started successfully
2. **"SeaweedFS filer is up"** - Filer ready  
3. **"SeaweedFS MQ broker is up"** - Broker started successfully
4. **Broker/Server logs** - Shown on broker startup failure

### Local Debugging
1. Run `./scripts/test-broker-startup.sh` to test broker startup
2. Check logs at `/tmp/weed-*.log` 
3. Test individual components:
   ```bash
   # Test master
   curl http://127.0.0.1:9333/cluster/status
   
   # Test filer  
   curl http://127.0.0.1:8888/status
   
   # Test broker
   nc -z 127.0.0.1 17777
   ```

### Common Issues
- **Broker fails to start**: Check filer is ready before starting broker
- **Gateway timeout**: Broker discovery fails, check broker is accessible  
- **Test hangs**: Timeout protection not working, reduce timeout values

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kafka Client  │───▶│  Kafka Gateway  │───▶│ SeaweedMQ Broker│
│   (Sarama,      │    │   (Protocol     │    │   (Message      │
│    kafka-go)    │    │    Handler)     │    │   Persistence)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                      │
                                ▼                      ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ SeaweedFS Filer │    │ SeaweedFS Master│
                       │ (Offset Storage)│    │ (Coordination)  │
                       └─────────────────┘    └─────────────────┘
                                │                      │
                                ▼                      ▼  
                       ┌─────────────────────────────────────────┐
                       │        SeaweedFS Volumes                │
                       │      (Message Storage)                  │
                       └─────────────────────────────────────────┘
```

This architecture ensures full integration testing of the entire Kafka → SeaweedFS message path.