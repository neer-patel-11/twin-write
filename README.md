# Twin-Write 

## Overview

**Twin-Write** is a modular system for handling SQL query execution using Redis as a queue, MySQL as the database, and Kafka for event-driven processing. The system consists of three main components:

1. **API Service** – Accepts SQL queries and queues them in Redis.
2. **Producer Service** – Fetches queries from Redis, executes them in MySQL, and publishes them to Kafka.
3. **Consumer Service** – Listens to Kafka for executed queries and logs them accordingly.

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

### Processing Queries

- The **Producer Service** will fetch queries from Redis, execute them in MySQL, and publish them to Kafka.
- The **Consumer Service** will consume executed queries from Kafka and log them.

---

## Future Improvements

- Add authentication & authorization.
- Implement error handling and retries.
- Extend support for multiple database engines.
- Add a UI dashboard to monitor query processing.

---

## Contributing

Feel free to fork, create issues, and submit pull requests. Contributions are welcome!

---

## License

MIT License © 2025 Neer Patel & Prit Dholariya



