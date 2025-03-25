package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"golang.org/x/net/context"
)

// Database and Kafka clients
var db *sql.DB
var kafkaProducer sarama.SyncProducer
var redisClient *redis.Client

func main() {
	// Load environment variables
	err := godotenv.Load("../.env")
	if err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Initialize Redis
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "yourredispassword")

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       0,
	})

	// Initialize MySQL
	dbUser := getEnv("DB_USER_1", "root")
	dbPass := getEnv("DB_PASSWORD_1", "password")
	dbHost := getEnv("DB_HOST_1", "localhost:3306")
	dbName := getEnv("DB_NAME_1", "mydatabase")

	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPass, dbHost, dbName)
	db, err = sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Ping DB to verify connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	fmt.Println("Connected to MySQL database!")

	// Initialize Kafka
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	kafkaProducer, err = initKafkaProducer(kafkaBroker)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Start query processing loop
	processQueries()
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// processQueries fetches queries from Redis, executes them, and sends them to Kafka
func processQueries() {
	ctx := context.Background()

	for {
		// Fetch multiple queries in batch
		queries, err := redisClient.LRange(ctx, "query_queue", 0, 9).Result()
		if err != nil {
			log.Printf("Error fetching queries from Redis: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(queries) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Printf("Processing %d queries\n from redis queue", len(queries))
		// Execute queries in batch
		err = executeBatchQueries(queries)
		if err != nil {
			log.Printf("Error executing batch queries: %v", err)
		}

		// Remove processed queries from queue
		_, err = redisClient.LTrim(ctx, "query_queue", int64(len(queries)), -1).Result()
		if err != nil {
			log.Printf("Error removing processed queries from Redis: %v", err)
		}

		// Publish queries to Kafka
		for _, query := range queries {
			err = publishToKafka("executed-queries", query)
			if err != nil {
				log.Printf("Failed to publish query to Kafka: %v", err)
			}
		}
	}
}

// executeBatchQueries runs a batch of SQL queries
func executeBatchQueries(queries []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, query := range queries {
		_, err := tx.Exec(query)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error executing query: %s, error: %v", query, err)
		}
	}

	return tx.Commit()
}

// initKafkaProducer initializes a Kafka producer
func initKafkaProducer(broker string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

// publishToKafka sends a message to a Kafka topic
func publishToKafka(topic, message string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := kafkaProducer.SendMessage(msg)
	return err
}
