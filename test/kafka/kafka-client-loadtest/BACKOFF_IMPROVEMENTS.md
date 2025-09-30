# Backoff Improvements for Schema Validation Failures

## Problem
When schema validation consistently fails, producers and consumers were hammering the Kafka Gateway with requests in tight loops, potentially overloading the system.

## Solution
Added progressive backoff delays in error handling paths to reduce load on the gateway when schema validation fails repeatedly.

## Changes Made

### 1. Kafka Gateway Backoff (weed/mq/kafka/protocol/produce.go)

**Added delays in multiple schema validation error paths:**

**Produce V0/V1 Handler:**
```go
if err != nil {
    // Check if this is a schema validation error and add delay to prevent overloading
    if h.isSchemaValidationError(err) {
        Debug("Schema validation failed for topic %s: %v - adding delay to prevent gateway overload", topicName, err)
        time.Sleep(200 * time.Millisecond) // Brief delay for schema validation failures
    }
    errorCode = 1 // UNKNOWN_SERVER_ERROR
}
```

**Produce V2+ Handler:**
```go
if prodErr != nil {
    // Check if this is a schema validation error and add delay to prevent overloading
    if h.isSchemaValidationError(prodErr) {
        Debug("Schema validation failed for topic %s: %v - adding delay to prevent gateway overload", topicName, prodErr)
        time.Sleep(200 * time.Millisecond) // Brief delay for schema validation failures
    }
    errorCode = 1 // UNKNOWN_SERVER_ERROR
    break
}
```

**Schema Validation Functions:**
```go
// In performSchemaValidation()
if h.isStrictSchemaValidation() {
    // Add delay before returning schema validation error to prevent overloading
    time.Sleep(100 * time.Millisecond)
    return fmt.Errorf("topic %s requires schema but no expected schema found: %w", topicName, err)
}

// In schema decoding
if err != nil {
    // Add delay before returning schema decoding error to prevent overloading
    time.Sleep(100 * time.Millisecond)
    return 0, fmt.Errorf("failed to decode schematized key/value: %w", err)
}
```

**Helper Function:**
```go
// isSchemaValidationError checks if an error is related to schema validation
func (h *Handler) isSchemaValidationError(err error) bool {
    if err == nil {
        return false
    }
    errStr := strings.ToLower(err.Error())
    return strings.Contains(errStr, "schema") ||
        strings.Contains(errStr, "decode") ||
        strings.Contains(errStr, "validation") ||
        strings.Contains(errStr, "registry") ||
        strings.Contains(errStr, "avro") ||
        strings.Contains(errStr, "protobuf") ||
        strings.Contains(errStr, "json schema")
}
```

### 2. Producer Backoff (internal/producer/producer.go)

**Before:**
```go
if p.isCircuitBreakerError(err) {
    p.consecutiveFailures++
    // Immediately continue to next iteration - tight loop!
    if p.consecutiveFailures >= 3 {
        return fmt.Errorf("circuit breaker is open...")
    }
}
```

**After:**
```go
if p.isCircuitBreakerError(err) {
    p.consecutiveFailures++
    
    // Progressive backoff delay to avoid overloading the gateway
    backoffDelay := time.Duration(p.consecutiveFailures) * 500 * time.Millisecond
    log.Printf("Producer %d: Backing off for %v to avoid overloading gateway", p.id, backoffDelay)
    
    select {
    case <-time.After(backoffDelay):
        // Continue after delay
    case <-ctx.Done():
        return nil
    }
    
    if p.consecutiveFailures >= 3 {
        return fmt.Errorf("circuit breaker is open...")
    }
}
```

**Backoff Schedule:**
- 1st failure: 500ms delay
- 2nd failure: 1000ms delay  
- 3rd failure: 1500ms delay, then stop

### 2. Consumer Backoff (internal/consumer/consumer.go)

**Sarama Consumer:**
```go
if err := h.consumer.processMessage(...); err != nil {
    log.Printf("Consumer %d: Error processing message: %v", h.consumer.id, err)
    h.consumer.metricsCollector.RecordConsumerError()
    
    // Add a small delay for schema validation or other processing errors
    select {
    case <-time.After(100 * time.Millisecond):
        // Continue after brief delay
    case <-session.Context().Done():
        return nil
    }
}
```

**Confluent Consumer:**
```go
if err := c.processMessage(...); err != nil {
    log.Printf("Consumer %d: Error processing message: %v", c.id, err)
    c.metricsCollector.RecordConsumerError()
    
    // Add a small delay for schema validation or other processing errors
    select {
    case <-time.After(100 * time.Millisecond):
        // Continue after brief delay
    case <-ctx.Done():
        return
    }
}
```

## Benefits

1. **Reduced System Load**: Prevents tight loops that can overwhelm the Kafka Gateway
2. **Progressive Backoff**: Longer delays for repeated failures, allowing time for issues to resolve
3. **Graceful Degradation**: System remains responsive even when schema validation fails
4. **Context Awareness**: Respects cancellation signals during backoff periods
5. **Logging**: Clear visibility into backoff behavior for debugging

## Expected Behavior

When schema enforcement is enabled but schemas are missing:

1. **Without backoff** (old behavior):
   ```
   Producer 0: Failed to produce message: circuit breaker is open
   Producer 0: Failed to produce message: circuit breaker is open
   Producer 0: Failed to produce message: circuit breaker is open
   [Continuous tight loop - high CPU usage]
   ```

2. **With backoff** (new behavior):
   ```
   Producer 0: Failed to produce message: circuit breaker is open
   Producer 0: Circuit breaker error detected (1/3 consecutive failures)
   Producer 0: Backing off for 500ms to avoid overloading gateway
   [500ms delay]
   Producer 0: Failed to produce message: circuit breaker is open
   Producer 0: Circuit breaker error detected (2/3 consecutive failures)
   Producer 0: Backing off for 1s to avoid overloading gateway
   [1000ms delay]
   Producer 0: Failed to produce message: circuit breaker is open
   Producer 0: Circuit breaker error detected (3/3 consecutive failures)
   Producer 0: Backing off for 1.5s to avoid overloading gateway
   [1500ms delay]
   Producer 0: Circuit breaker is open - stopping producer after 3 consecutive failures
   ```

## Testing

The improvements can be observed by running the quick-test with schema enforcement:

```bash
make quick-test
```

Look for log messages containing:
- "Backing off for X to avoid overloading gateway"
- "Circuit breaker error detected"
- Progressive delay increases

This ensures the system remains stable and responsive even when schema validation consistently fails.
