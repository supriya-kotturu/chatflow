# CS6650 Assignment 3: Persistence and Data Management

## Overview

Building on Assignment 2, we now add persistent storage to your chat system to maintain message history and enable analytics. The challenge is to write to the database at high speed while maintaining system performance and not creating bottlenecks in your message pipeline.

This assignment will test your ability to handle high-throughput database writes while maintaining the real-time nature of your chat system.

## Part 1: Database Design and Implementation

You are responsible to choose a database with tradeoffs to support the below queries.

### Data Model Requirements

Your schema must efficiently support these queries:

#### Core Queries

1. **Get messages for a room in time range**
   
   - Input: roomId, startTime, endTime
   - Output: Ordered list of messages
   - Performance target: < 100ms for 1000 messages

2. **Get user's message history**
   
   - Input: userId, optional date range
   - Output: Messages across all rooms
   - Performance target: < 200ms

3. **Count active users in time window**
   
   - Input: startTime, endTime
   - Output: Unique user count
   - Performance target: < 500ms

4. **Get rooms user has participated in**
   
   - Input: userId
   - Output: List of roomIds with last activity
   - Performance target: < 50ms

#### Analytics Queries

1. Messages per seconds/minute statistics
2. Most active users (top N)
3. Most active rooms (top N)
4. User participation patterns

##### Metrics API

##### Create an API on your server which returns a JSON with the results of core queries and analytics queries.

Call this API after the test has ended from your client.

Log the results on your client.

Attach the screenshot of that log in the report.

### Part 2: Consumer Modifications

Update your consumer to persist messages efficiently:

#### 1. Optimal Batch Sizes

Test and document performance with:

- Batch size: 100, 500, 1000, 5000
- Flush interval: 100ms, 500ms, 1000ms
- Run up to 5 tests and choose the optimal
- Find optimal balance between latency and throughput

#### 2. Write-Behind Implementation

- Separate queue consumption from database writes
- Use separate thread pools:
  - Message consumers (from queue)
  - Database writers
  - Statistics aggregators

### Data Integrity

#### 1. Idempotent Writes

- Use message_id as unique identifier
- Handle duplicate messages gracefully
- Implement upsert operations where possible

#### 2. Error Recovery

- Implement dead letter queue for failed writes
- Exponential backoff for retries
- Circuit breaker for database failures

## Part 3: Performance Optimization

### Database Tuning

#### 1. Indexing Strategy

Document your index choices:

- Primary access patterns
- Index selectivity analysis
- Composite index decisions
- Impact on write performance

#### 2. Connection Pooling

Decide if you want to choose a connection pool for database connections

#### 3. Query Optimization

- Use prepared statements
- Implement query result caching
- Consider materialized views for analytics

### System Resilience

#### Option 1: Circuit Breaker Pattern

#### Option 2: Rate Limiting

#### Option 3: Caching Layer (Optional)

## Part 4: Load Testing

### Progressive Load Tests

#### Test 1: Baseline

- **Load**: 500,000 messages
- **Duration**: Run to completion
- **Metrics to collect**:
  - Write throughput (messages/sec)
  - Database CPU/Memory
  - Write latency percentiles
  - Queue depth stability

#### Test 2: Stress Test

- **Load**: 1,000,000 messages
- **Duration**: Run to completion
- **Goals**:
  - Identify bottlenecks
  - Find breaking points
  - Document degradation curve

#### Test 3: Endurance Test

- **Load**: Sustained rate for longer duration. Document the duration. Example could be 30 minutes.
- **Target rate**: 80% of maximum throughput
- **Monitor for**:
  - Memory leaks
  - Connection pool exhaustion
  - Disk space issues
  - Performance degradation over time

### Metrics Collection

#### Database Metrics

Monitor and record:

- Queries per second
- Active connections
- Lock wait time
- Buffer pool hit ratio
- Disk I/O statistics

## Submission Requirements

### 1. Repository Updates

Create new folders:

- `/database` - Schema files and setup scripts
- `/consumer-v3` - Updated consumer with persistence
- `/monitoring` - Metrics collection scripts
- `/load-tests` - Test configurations and results

### 2. Database Design Document (2 pages max)

Include:

- Database choice justification
- Complete schema design
- Indexing strategy
- Scaling considerations
- Backup and recovery approach

### 3. Performance Report

Provide detailed analysis of:

#### Write Performance

- Maximum sustained write throughput
- Latency percentiles (p50, p95, p99)
- Batch size optimization results
- Resource utilization

#### System Stability

- Queue depth graphs over time
- Database performance metrics
- Memory usage patterns
- Connection pool statistics

#### Bottleneck Analysis

- Identify primary bottlenecks
- Proposed solutions
- Trade-offs made

### 4. Configuration Files

Include all configuration:

- Database connection settings
- Thread pool configurations
- Batch processing parameters
- Circuit breaker thresholds

## Grading Rubric

### Database Implementation (10 points)

- Schema design appropriateness (4)
- Query optimization (3)
- Error handling (3)

### Design Documentation (5 points)

- Clarity and completeness (3)
- Design justification (2)

### Metrics API & Results Log (5 points)

### Performance Results (15 points)

- Sustained write throughput (7)
- System stability under load (4)
- Resource efficiency (4)

### Bonus Points

- Exceptional performance (+2 points)
- Innovative optimization techniques (+1 point)
- Comprehensive monitoring dashboard (+1 point)

## Deadline: 03/27/2026 5PM PST