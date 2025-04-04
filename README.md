# Twin-Write 
## Overview
**Twin-Write** is a dual-database consistency system designed to balance performance and reliability for applications requiring both immediate access to data and eventual consistency across multiple data stores. It implements a "write-to-both" pattern with the following key features:

- **Immediate Consistency**: Direct writes to the primary database for instant data access
- **Eventual Consistency**: Asynchronous propagation to secondary systems via queue-based processing
- **Decoupled Architecture**: Separates read/write concerns for improved scalability and resilience

The system consists of three main components:
1. **API Service** – Accepts SQL queries, executes them immediately on the primary database, and queues them in Redis for secondary processing.
2. **Producer Service** – Fetches queries from Redis and publishes them to Kafka for reliable distribution and replay capability.
3. **Consumer Service** – Listens to Kafka and applies the same queries to secondary databases, ensuring eventual consistency.

### Use Cases

#### 1. High-Throughput OLTP Systems with Analytics Requirements
For applications that need to balance transactional performance with analytical capabilities:
- Transaction data is immediately written to the OLTP database
- The same data is asynchronously propagated to OLAP systems for analytics
- Provides separation of concerns between operational and analytical workloads

#### 2. Microservice Data Synchronization
When multiple microservices maintain their own databases but need shared data:
- Services write to their primary database for immediate use
- Twin-Write ensures the same data eventually propagates to other service databases
- Reduces tight coupling between services while maintaining data consistency

#### 3. Disaster Recovery and Geographic Distribution
For applications requiring data redundancy across regions:
- Primary writes occur in the main region's database
- Twin-Write ensures backups or geographically distributed replicas stay in sync
- Provides resilience against regional outages

#### 4. Cache Warming and Search Indexing
For systems with performance-critical search or caching layers:
- Write transactions to the primary database first
- Twin-Write handles propagation to search indices (like Elasticsearch) or cache systems
- Ensures search results and caches eventually reflect the latest data


---

## Project Structure
```
/twin-write
│-- /api          # API service to queue SQL queries
│-- /producer     # Fetches, executes queries, and publishes events
│-- /consumer     # Consumes executed queries from Kafka
│-- .gitignore    # Ignore sensitive files
│-- README.md     # Project documentation
```

---

## Technologies Used
- **Go (Golang)** – For building API, producer, and consumer services.
- **Redis** – Used as a queue for SQL queries.
- **MySQL** – Database for executing queries.
- **Kafka** – For asynchronous message passing.
- **Docker** – Optional for containerizing the services.

---

## System Architecture & Design


Twin-Write uses a distributed architecture with the following data flow:
1. API Service receives SQL queries through HTTP endpoints
2. Queries are queued in Redis for asynchronous processing
3. Producer Service implements an auto-scaling mechanism to dynamically adjust worker count based on queue size
4. Queries are executed in MySQL in batched transactions
5. Execution events are published to Kafka
6. Consumer Service processes the execution events from Kafka

### Key Design Features

#### Auto-scaling Producer System
The Producer Service implements dynamic scaling based on queue load:
- **Auto-scaling**: Automatically adjusts the number of producer workers based on queue size
- **Configurable thresholds**: Defines parameters for scaling decisions
- **Batched processing**: Processes queries in batches for improved throughput
- **Transactional integrity**: Executes batches within database transactions

#### Redis Queue Implementation
- Atomic batch retrieval with Lua scripting to ensure thread safety
- RPUSH for adding queries, LRANGE+LTRIM for batch retrieval
- Queue monitoring for scaling decisions

#### Kafka Integration
- Synchronous producers ensure message delivery confirmation
- Topic-based separation of concerns
- Event-driven architecture for system extensibility

---

## Implementation Details

### API Service
The API Service is a lightweight HTTP server that:
- Accepts SQL queries via a POST endpoint at `/query`
- Validates incoming query requests
- Pushes valid queries to a Redis list called `query_queue`
- Returns appropriate HTTP status codes based on operation success

```go
// Key components:
// - HTTP server with /query endpoint
// - Redis client for queue operations
// - Request validation and error handling
```

### Producer Service
The Producer Service is responsible for query execution and implements a sophisticated auto-scaling mechanism:

#### Auto-scaling Logic
- **Queue Monitoring**: Periodically checks Redis queue size
- **Dynamic Worker Scaling**: Adds or removes workers based on queue load
- **Controlled Scaling**: Respects minimum and maximum worker counts
- **Idle Timeout**: Prevents rapid scaling oscillations

#### Query Processing Workflow
1. Atomically fetches query batches from Redis
2. Executes queries in MySQL within a single transaction
3. Publishes execution events to Kafka
4. Implements timeout handling and graceful worker termination

```go
// Configuration constants
const (
    batchSize           = 10      // Number of queries to process in a batch
    queueSizeThreshold  = 100     // Add a producer when queue grows by this amount
    maxProducers        = 10      // Maximum number of producers
    minProducers        = 1       // Minimum number of producers
    scaleCheckInterval  = 5       // Check for scaling every N seconds
    processingTimeout   = 30      // Maximum time (seconds) to process a batch
    idleTimeout         = 5       // Time (seconds) to wait before scaling down
)
```

### Consumer Service
The Consumer Service:
- Connects to Kafka to listen for executed query events
- Logs execution results
- Can be extended to implement advanced monitoring or auditing

---

## Performance Optimization

### Database Connection Pooling
The Producer Service implements database connection pooling:
```go
// Configure connection pool
db.SetMaxOpenConns(maxProducers * 2) // Allow room for concurrent transactions
db.SetMaxIdleConns(maxProducers)
db.SetConnMaxLifetime(time.Hour)
```

### Batch Processing
- Queries are processed in configurable batch sizes
- Atomic batch retrieval using Redis Lua scripts
- Transaction-based execution for improved throughput and consistency

### Redis Performance Considerations
- Atomicity and race condition prevention with Lua scripting
- Efficient queue operations with O(1) complexity

---

## Installation & Setup
### Prerequisites
Ensure you have the following installed:
- [Go](https://golang.org/doc/install)
- [Redis](https://redis.io/docs/getting-started/)
- [MySQL](https://dev.mysql.com/downloads/)
- [Apache Kafka](https://kafka.apache.org/quickstart)

### 1. Clone the repository
```sh
git clone https://github.com/neer-patel-11/twin-write.git
cd twin-write
```

### 2. Set Up Environment Variables
Create a `.env` file in each service (`api`, `producer`, `consumer`) with the following:
```env
# MySQL Configuration
DB_USER_1=root
DB_PASSWORD_1=password
DB_HOST_1=localhost:3306
DB_NAME_1=mydatabase
# Redis Configuration
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
# Kafka Configuration
KAFKA_BROKER=localhost:9092
SERVER_PORT=8080
```

### 3. Run Services
#### Start API Service
```sh
cd api
go run main.go
```

#### Start Producer Service
```sh
cd producer
go run main.go
```

#### Start Consumer Service
```sh
cd consumer
go run main.go
```

---

## Usage
### Sending a Query (API Request)
```sh
curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query": "INSERT INTO users (name, email) VALUES (\"John Doe\", \"john@example.com\")"}'
```
This request will add the query to the Redis queue.

### Load Testing
A simple Python script is provided for load testing:

```python
import aiohttp
import asyncio

async def send_request(session, url, payload):
    async with session.post(url, json=payload) as response:
        print(f"Response sent")

async def main():
    url = "http://localhost:8080/query"
    payload = {
        "query": "INSERT INTO users (name, email, password_hash) VALUES ('abc', 'abc@example.com', 'hashed_password212_here');"
    }
    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, url, payload) for _ in range(10000)]
        await asyncio.gather(*tasks)

asyncio.run(main())
```

### Processing Queries
- The **Producer Service** will fetch queries from Redis, execute them in MySQL, and publish them to Kafka.
- The **Consumer Service** will consume executed queries from Kafka and log them.

---

## Error Handling & Reliability
- Transaction rollbacks on query execution failures
- Configurable processing timeouts
- Producer auto-scaling to handle load spikes
- Graceful shutdown mechanisms

---

## Contributing
Feel free to fork, create issues, and submit pull requests. Contributions are welcome!

---

## License
MIT License © 2025 Neer Patel & Prit Dholariya
