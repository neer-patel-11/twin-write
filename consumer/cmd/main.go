package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

// Database connection
var db *sql.DB

func main() {
	// Load environment variables
	err := godotenv.Load("../.env")
	if err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Get database configuration
	dbUser := getEnv("DB_USER_1", "root")
	dbPass := getEnv("DB_PASSWORD_1", "password")
	dbHost := getEnv("DB_HOST_1", "localhost:3306")
	dbName := getEnv("DB_NAME_1", "mydatabase")
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	topic := "executed-queries"

	// Initialize database connection
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPass, dbHost, dbName)
	db, err = sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test DB connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	fmt.Println("Connected to MySQL database!")

	// Start Kafka consumer
	consumeKafkaMessages(kafkaBroker, topic)
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// consumeKafkaMessages listens to a Kafka topic and executes SQL queries
func consumeKafkaMessages(broker, topic string) {
	consumer, err := sarama.NewConsumer([]string{broker}, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start Kafka partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Listening for messages on Kafka topic:", topic)

	for message := range partitionConsumer.Messages() {
		query := string(message.Value)
		fmt.Printf("Received query from Kafka: %s\n", query)

		// Execute SQL query
		if err := executeQuery(query); err != nil {
			log.Printf("Failed to execute query: %v", err)
		} else {
			fmt.Println("Query executed successfully!")
		}
	}
}

// executeQuery runs the provided SQL query
func executeQuery(query string) error {
	_, err := db.Exec(query)
	return err
}
