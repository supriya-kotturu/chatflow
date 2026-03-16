# CS6650 Assignment 2: Adding Message Distribution and Queue Management

## Overview

Building on Assignment 1, we'll now implement real message distribution using message queues. Instead of just echoing messages back, your server will distribute messages to all connected users in the same room through a message queue system.

This assignment transforms your simple echo server into a proper chat system where messages are broadcast to all participants in a room, introducing the complexities of distributed message delivery and queue management.

## Part 1: Implement Message Queue Integration

### Server Modifications

Modify your WebSocket server to become a message producer:

#### 1. Message Publishing

When receiving a valid message, publish it to a message queue instead of echoing back.

Queue message format:

```json
{
  "messageId": "UUID",
  "roomId": "string",
  "userId": "string",
  "username": "string", 
  "message": "string",
  "timestamp": "ISO-8601",
  "messageType": "TEXT|JOIN|LEAVE",
  "serverId": "string",
  "clientIp": "string"
}
```

#### 2. Queue Configuration

**Option A: RabbitMQ**

- Deploy RabbitMQ on a separate EC2 instance
- Create a topic exchange named `chat.exchange`
- Routing key pattern: `room.{roomId}`
- One queue per room (room.1 through room.20)
- Configure message TTL and queue limits

**Option B: AWS SQS**

- Create FIFO queues for message ordering
- One queue per room
- Configure visibility timeout and retention period
- Set up appropriate IAM roles

#### 3. Connection Management

- Implement connection pooling for queue connections
- Thread-safe channel/client management
- Graceful handling of queue unavailability
- Circuit breaker for queue failures

### Implementation Requirements

For RabbitMQ:

```java
// Example channel pool initialization
public class ChannelPool {
    private final BlockingQueue<Channel> pool;
    private final Connection connection;

    public void init() {
        // Create connection in init/constructor
        // Pre-create channels for pool
    }

    public Channel borrowChannel() {
        // Get channel from pool
    }

    public void returnChannel(Channel channel) {
        // Return channel to pool
    }
}
```

## Part 2: Implement Message Consumers

Build a separate consumer application that distributes messages to connected clients:

### Consumer Design

#### 1. Multi-threaded Consumer Pool

- Configurable number of consumer threads
- Each consumer handles specific rooms
- Fair distribution of rooms across consumers
- Dynamic scaling based on load

#### 2. Message Processing Pipeline

```
Queue -> Consumer -> Room Manager -> WebSocket Broadcaster
```

- Pull messages from queue
- Route to appropriate room manager
- Broadcast to all connected clients in room
- Acknowledge message after successful broadcast

#### 3. State Management

Maintain state for:

- Active rooms and their participants
- User session mapping
- Message delivery tracking
- Consumer health metrics

Thread-safe data structures:

```java
ConcurrentHashMap<String, Set<WebSocketSession>> roomSessions;
ConcurrentHashMap<String, UserInfo> activeUsers;
AtomicLong messagesProcessed;
```

#### 4. Delivery Guarantees

- Implement at-least-once delivery
- Handle duplicate messages
- Message ordering within rooms
- Failed delivery retry logic

### Consumer Deployment

- Run on separate EC2 instance
- Configure for remote queue connection
- Implement health checks
- Auto-restart on failure

## Part 3: Load Balancing

Implement load balancing for your WebSocket servers:

### AWS Application Load Balancer Setup

#### 1. ALB Configuration

- Create target groups for WebSocket servers
- Configure sticky sessions (required for WebSocket)
- Health check configuration:
  - Path: `/health`
  - Interval: 30 seconds
  - Timeout: 5 seconds
  - Healthy threshold: 2
  - Unhealthy threshold: 3

#### 2. WebSocket Support

- Enable WebSocket protocol
- Configure idle timeout (> 60 seconds)
- Session affinity cookie duration

#### 3. Deployment Architecture

```
Client -> ALB -> [Server1, Server2, Server3, Server4] -> Queue -> Consumers
```

#### 4. Testing Load Distribution

- Monitor connection distribution
- Verify sticky session behavior
- Test failover scenarios
- Measure impact on latency

## Part 4: System Tuning

### Performance Optimization Goals

#### 1. Queue Management

**Target Metrics:**

- Queue depth < 1000 messages consistently
- Consumer lag < 100ms
- No message loss under load

**Tuning Parameters:**

- Prefetch count
- Consumer concurrency
- Batch acknowledgment size
- Connection pool size

#### 2. Threading Optimization

**Client Configuration:**

- Test with: 64, 128, 256, 512 threads
- Find optimal thread count for maximum throughput
- Balance connections across servers

**Consumer Configuration:**

- Test with: 10, 20, 40, 80 consumer threads
- Optimal batch processing size
- Queue polling strategy

#### 3. Monitoring Implementation

Track and log:

- Messages per second (in/out)
- Queue depth over time
- Consumer lag
- Connection count per server
- Error rates

### Queue Profile Analysis

Ideal queue depth patterns:

**Good Profile (Stable plateau):**

```
Messages
  ^
  |    ___________
  |   /
  |  /
  +-----------------> Time
```

**Bad Profile (Sawtooth):**

```
Messages
  ^
  |    /\    /\
  |   /  \  /  \
  |  /    \/    \
  +-----------------> Time
```

## Performance Testing

### Test Scenarios

#### 1. Single Server Baseline

- 500K messages
- Record throughput and latency
- Monitor queue metrics
- Document resource usage

#### 2. Load Balanced (2 instances)

- Same 500K messages
- Compare throughput improvement
- Check even distribution
- Monitor queue behavior

#### 3. Load Balanced (4 instances)

- 500K messages
- Maximum throughput test
- Stress test with 1M messages
- Identify bottlenecks

### Metrics Collection

For each test run, collect:

1. **Client Metrics:**
   
   - Total runtime
   - Messages per second
   - Connection failures
   - Retry counts

2. **Queue Metrics:**
   
   - Peak queue depth
   - Average queue depth
   - Consumer rate
   - Producer rate

3. **System Metrics:**
   
   - CPU usage (all instances)
   - Memory usage
   - Network I/O
   - Disk I/O (if applicable)

## Submission Requirements

Submit your work to Canvas as a PDF containing:

### 1. Git Repository URL

Create new folders:

- `/server-v2` - Updated server with queue integration
- `/consumer` - Consumer application
- `/deployment` - ALB configuration, scripts
- `/monitoring` - Monitoring scripts and tools

### 2. Architecture Document (2 pages max)

Include:

- System architecture diagram
- Message flow sequence diagram
- Queue topology design
- Consumer threading model
- Load balancing configuration
- Failure handling strategies

### 3. Test Results

#### Single Instance Tests:

- Screenshot of client output (throughput metrics)
- RabbitMQ Management Console showing:
  - Queue depths over time
  - Message rates (publish/consume)
  - Connection details

#### Load Balanced Tests:

- Client output for 2 and 4 instances
- ALB metrics showing request distribution
- Queue metrics comparison
- Performance improvement analysis

### 4. Configuration Details

- Queue configuration parameters
- Consumer configuration
- ALB settings
- Instance types used

## Grading Rubric

### Queue Integration (15 points)

- Correct message publishing (5)
- Proper consumer implementation (5)
- Error handling and recovery (3)
- Clean separation of concerns (2)

### System Design (10 points)

- Clear architecture documentation
- Appropriate design patterns
- Scalability considerations

### Single Instance Performance (10 points)

- Achieves good throughput (5)
- Maintains stable queue depths (3)
- Proper resource utilization (2)

### Load Balanced Performance (10 points)

- Improved throughput over single instance (5)
- Even distribution across instances (3)
- Stable system under load (2)

### Bonus Points

- Fastest 3 implementations (+2 points each)
- Next fastest 3 implementations (+1 point each)
- Must maintain stable queue profiles

## Deadline: 03/08/2026 5PM PST

## Additional Resources

### RabbitMQ Best Practices

#### Connection Management

```java
// Singleton connection
private static Connection connection;

// Channel per thread
ThreadLocal<Channel> channelThreadLocal = new ThreadLocal<>();

// Or use channel pooling
BlockingQueue<Channel> channelPool = new ArrayBlockingQueue<>(poolSize);
```

#### Reliability Settings

```java
// Publisher confirms
channel.confirmSelect();

// Mandatory flag
channel.basicPublish(exchange, routingKey, 
    MessageProperties.PERSISTENT_TEXT_PLAIN, 
    message.getBytes());

// Consumer acknowledgment
channel.basicConsume(queueName, false, consumer);
```

### AWS SQS Alternative

If using SQS instead of RabbitMQ:

```java
// Batch sending
SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
    .queueUrl(queueUrl)
    .entries(messageBatch)
    .build();

// Long polling
ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
    .queueUrl(queueUrl)
    .maxNumberOfMessages(10)
    .waitTimeSeconds(20)
    .build();
```

### Monitoring Tools

#### RabbitMQ Management

- Enable management plugin
- Default port: 15672
- Monitor queue depth trends
- Set up alerts for high queue depth

#### CloudWatch (for AWS)

- ALB metrics
- SQS metrics (if using)
- Custom metrics from application

#### Application Metrics

```java
// Micrometer example
@Timed("message.processing")
public void processMessage(Message msg) {
    meterRegistry.counter("messages.processed").increment();
    // Process message
}
```

### Common Issues and Solutions

**Issue**: Growing queue depth

- **Solution**: Add more consumers, optimize processing

**Issue**: Uneven load distribution

- **Solution**: Check ALB sticky session configuration

**Issue**: Connection pool exhaustion

- **Solution**: Increase pool size, implement proper cleanup

**Issue**: Message ordering problems

- **Solution**: Use FIFO queues or partition by room

### Performance Tuning Checklist

- [ ] Queue prefetch optimized
- [ ] Connection pooling implemented
- [ ] Batch processing where applicable
- [ ] Appropriate thread pool sizes
- [ ] Circuit breakers in place
- [ ] Monitoring and alerting configured
- [ ] Graceful shutdown implemented
- [ ] Resource limits configured