package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"golang.org/x/net/context"
)

// QueryRequest represents the incoming query request
type QueryRequest struct {
	Query string `json:"query"`
}

// Redis client
var redisClient *redis.Client

func main() {
	// Load environment variables from .env file
	err := godotenv.Load("../.env")
	if err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Get Redis configuration
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "yourredispassword")

	// Initialize Redis client
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       0,
	})

	// Register query handler
	http.HandleFunc("/query", queryHandler)

	// Start server
	port := getEnv("SERVER_PORT", "8080")
	fmt.Printf("API Server starting on port %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// queryHandler pushes SQL queries to Redis queue
func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request QueryRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing request: %v", err), http.StatusBadRequest)
		return
	}

	if request.Query == "" {
		http.Error(w, "No SQL query provided", http.StatusBadRequest)
		return
	}

	// Push query to Redis queue
	fmt.Printf("Adding query to redis queue: %s\n", request.Query)
	ctx := context.Background()
	err = redisClient.RPush(ctx, "query_queue", request.Query).Err()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to queue query: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Query added to queue successfully"))
}
