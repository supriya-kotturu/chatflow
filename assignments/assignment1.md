# CS6650 Assignment 1: Building a WebSocket Chat Server and Client

## Overview

You work for ChatFlow - a startup building enterprise-grade chat infrastructure for companies worldwide. ChatFlow aims to provide a scalable, distributed messaging platform that can handle millions of concurrent users across different organizations.

Through this series of assignments, you'll build a scalable distributed cloud-based chat system that can handle real-time messaging at scale. The system will enable:

- Real-time message delivery between users
- Message history and persistence
- User presence tracking
- Analytics on chat usage patterns
- Support for multiple chat rooms/channels

In Assignment 1, we'll build a WebSocket server and a multithreaded client that can simulate thousands of users sending messages. The server will accept connections, validate messages, and echo them back. In Assignment 2, we'll add message distribution through queues. In Assignment 3, we'll add persistence. In Assignment 4, we'll implement retrieval APIs and perform comprehensive load testing.

## Part 1: Implement the WebSocket Server

### Server Specifications

Create a WebSocket server using Java that implements the following endpoints:

**WebSocket Endpoint: `/chat/{roomId}`**

The server should:

1. Accept WebSocket connections with a room ID parameter

2. Validate incoming JSON messages with the following structure:
   
   ```json
   {
   "userId": "string (1-100000)",
   "username": "string (3-20 chars)",
   "message": "string (1-500 chars)",
   "timestamp": "ISO-8601 timestamp",
   "messageType": "TEXT|JOIN|LEAVE"
   }
   ```

3. Perform validation:
   
   - userId must be between 1 and 100000
   - username must be 3-20 alphanumeric characters
   - message must be 1-500 characters
   - timestamp must be valid ISO-8601
   - messageType must be one of the specified values

4. For valid messages, echo back to sender with a server timestamp and status

5. Return appropriate error messages for invalid requests

**REST Endpoint: `/health`**

- Simple GET endpoint returning server status

### Implementation Requirements

- Use Java with either:
  - Java-WebSocket library
  - Jakarta WebSocket (formerly javax.websocket)
  - Spring WebSocket
- Implement proper connection management
- Thread-safe message handling
- Deploy on AWS EC2 free tier instance in us-west-2

### Testing

Test your WebSocket server using:

- [wscat](https://github.com/websockets/wscat) command-line tool
- Postman's WebSocket support
- Browser-based WebSocket testing tools

## Part 2: Build the Multithreaded WebSocket Client

### Client Requirements

Build a Java client that simulates a high-volume chat system:

#### 1. Connection Management

- Create WebSocket connections to your server
- Implement connection pooling for efficiency
- Handle reconnection on failure

#### 2. Multithreading Design

**Initial Phase:**

- Create 32 threads at startup
- Each thread establishes a WebSocket connection
- Each thread sends 1000 messages then terminates
- Measure this phase separately as "warmup"

**Main Phase:**

- After initial threads complete, you're free to create optimal thread configuration
- Continue until all 500K messages are sent
- Threads should maintain persistent WebSocket connections where possible

#### 3. Message Generation Thread

- Single dedicated thread generates all messages

- Places messages in a thread-safe queue/buffer

- Ensures sending threads never wait for messages

- Minimize CPU and memory usage
  
  #### 3.1 Message Generation
  
  Generate 500,000 chat messages total with random data:
  
  - userId: random between 1-100000
  - username: generate from userId (e.g., "user12345")
  - message: random from a pool of 50 pre-defined messages
  - roomId: random between 1-20
  - messageType: 90% TEXT, 5% JOIN, 5% LEAVE
  - timestamp: current time

#### 4. Error Handling

- Retry failed sends up to 5 times with exponential backoff
- Track failed messages after 5 retries
- Handle connection drops gracefully

### Performance Metrics

Upon completion, output:

1. Number of successful messages sent
2. Number of failed messages
3. Total runtime (wall time)
4. Overall throughput (messages/second)
5. Connection statistics (total connections, reconnections)

### Little's Law Analysis

Before implementing, calculate expected throughput using Little's Law:

- Measure single message round-trip time
- Factor in connection overhead
- Predict maximum throughput
- Compare actual results to predictions

## Part 3: Performance Analysis

Enhance your client to collect detailed metrics:

### Per-Message Metrics

For each message:

1. Record timestamp before send
2. Record timestamp when acknowledgment received
3. Calculate latency in milliseconds
4. Write to CSV: `{timestamp, messageType, latency, statusCode, roomId}`

### Statistical Analysis

After test completion, calculate and display:

- Mean response time (ms)
- Median response time (ms)
- 95th percentile response time
- 99th percentile response time
- Min/max response times
- Throughput per room
- Message type distribution

### Visualization

Create a simple line chart showing throughput over time (messages/second in 10-second buckets)

## ## Submission Requirements

Submit as PDF to Canvas containing:

### 1. Git Repository URL with:

- `/server` - Server implementation with deployment instructions
- `/client-part1` - Basic load testing client
- `/client-part2` - Client with performance analysis
- `/results` - Test results and analysis
- Include README files with clear running instructions

### 2. Design Document (2 pages max):

- Architecture diagram
- Major classes and their relationships
- Threading model explanation
- WebSocket connection management strategy
- Little's Law calculations and predictions

### 3. Test Results:

- Screenshot of Part 1 output (basic metrics)
- Screenshot of Part 2 output (detailed metrics)
- Performance analysis charts
- Evidence of EC2 deployment (EC2 console screenshot)

## Grading Rubric

### Server Implementation (10 points)

- Correct WebSocket handling (5)
- Proper validation and error handling (3)
- Clean code and documentation (2)

### Client Design (5 points)

- Clear architecture description
- Appropriate design patterns

### Client Part 1 (10 points)

- Achieves close to theoretical throughput
- Correct thread management

### Client Part 2 (10 points)

- Accurate latency measurements (5)
- Statistical calculations (5)

### Part 3 (Visualization) (2 points)

- Simple line chart showing throughput over time

### Bonus Points

- Fastest 3 implementations (+2 points each)
- Next fastest 3 implementations (+1 point each)

## Deadline: 2/13/2026 5PM PST

## Additional Resources

### WebSocket Libraries for Java

**Java-WebSocket**: 

- Simple, lightweight library

- Maven dependency:
  
  ```xml
  <dependency>
    <groupId>org.java-websocket</groupId>
    <artifactId>Java-WebSocket</artifactId>
    <version>1.5.4</version>
  </dependency>
  ```

**Jakarta WebSocket**:

- Standard Java EE approach
- Good for servlet containers

**Spring WebSocket**:

- If using Spring Boot
- Excellent documentation and support

### Connection Management Tips

1. **Connection Pooling**: Reuse WebSocket connections across multiple messages
2. **Heartbeat/Ping**: Implement keep-alive mechanism
3. **Backpressure**: Handle server overload gracefully
4. **Circuit Breaker**: Implement circuit breaker pattern for resilience

### Performance Testing Tools

- **VisualVM**: Profile your Java application
- **JMeter WebSocket Plugin**: Alternative testing approach
- **Chrome DevTools**: WebSocket frame inspector

### AWS EC2 Setup

1. Launch t2.micro instance (free tier)
2. Configure Security Group:
   - Inbound: Port 8080 (or your chosen port)
   - Inbound: Port 22 for SSH
3. Install Java 11 or higher
4. Deploy your server JAR/WAR
5. Use `screen` or `systemd` for persistent running

### Common Pitfalls to Avoid

- Not handling WebSocket close events properly
- Forgetting to implement reconnection logic
- Not using connection pooling in client
- Blocking message generation thread
- Inadequate error handling for network issues
