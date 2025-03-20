package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/IBM/sarama"
)

// QueryRequest represents the incoming query request
type QueryRequest struct {
	Query string `json:"query"`
}

// QueryResponse represents the API response structure
type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
	Error   string                   `json:"error,omitempty"`
}

// Database connection
var db *sql.DB
var kafkaProducer sarama.SyncProducer

func main() {
	// Load environment variables from .env file
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Get database configuration from environment variables
	dbUser := getEnv("DB_USER_1", "root")
	dbPass := getEnv("DB_PASSWORD_1", "password")
	dbHost := getEnv("DB_HOST_1", "localhost:3306")
	dbName := getEnv("DB_NAME_1", "mydatabase")
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")

	// Get server port
	port, err := strconv.Atoi(getEnv("SERVER_PORT", "8080"))
	if err != nil {
		log.Fatalf("Invalid SERVER_PORT value: %v", err)
	}

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

	// Initialize Kafka producer
	kafkaProducer, err = initKafkaProducer(kafkaBroker)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Register query handler
	http.HandleFunc("/query", queryHandler)

	// Start server
	fmt.Printf("Server starting on port %d...\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// queryHandler executes SQL query and publishes it to Kafka
func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusBadRequest)
		return
	}

	var query string
	contentType := r.Header.Get("Content-Type")

	if contentType == "application/json" {
		var request QueryRequest
		err = json.Unmarshal(body, &request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error parsing JSON request: %v", err), http.StatusBadRequest)
			return
		}
		query = request.Query
	} else {
		query = string(body)
	}

	if query == "" {
		http.Error(w, "No SQL query provided", http.StatusBadRequest)
		return
	}

	fmt.Printf("Executing query: %s\n", query)

	// Execute SQL query
	results, err := executeQuery(query)

	// Prepare response
	response := QueryResponse{}
	if err != nil {
		response.Error = err.Error()
	} else {
		response.Results = results

		// Publish executed query to Kafka
		err = publishToKafka("executed-queries", query)
		if err != nil {
			log.Printf("Failed to publish query to Kafka: %v", err)
		} else {
			fmt.Println("Query published to Kafka successfully!")
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// executeQuery runs the provided SQL query
func executeQuery(query string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				switch v := val.(type) {
				case []byte:
					row[col] = string(v)
				default:
					row[col] = v
				}
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
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
