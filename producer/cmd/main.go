package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"golang.org/x/net/context"
)

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

// Database and Kafka clients
var db *sql.DB
var kafkaProducer sarama.SyncProducer
var redisClient *redis.Client

// Producer control
var producerWg sync.WaitGroup
var producerControl = make(chan bool)
var activeProducers int
var producerMutex sync.Mutex

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

	// Configure connection pool
	db.SetMaxOpenConns(maxProducers * 2) // Allow room for concurrent transactions
	db.SetMaxIdleConns(maxProducers)
	db.SetConnMaxLifetime(time.Hour)

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

	// Start initial producer
	startNewProducer(0)
	
	// Start queue monitor
	go monitorQueueSize()

	// Wait for producers to finish (this will block forever in practice)
	producerWg.Wait()
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// monitorQueueSize checks the queue size and scales producers accordingly
func monitorQueueSize() {
	ctx := context.Background()
	lastScaleDown := time.Now()
	
	for {
		// Get current queue size
		queueSize, err := redisClient.LLen(ctx, "query_queue").Result()
		if err != nil {
			log.Printf("Error checking queue size: %v", err)
			time.Sleep(time.Duration(scaleCheckInterval) * time.Second)
			continue
		}
		
		log.Printf("Current queue size: %d, Active producers: %d", queueSize, activeProducers)
		
		// Scale up/down based on queue size
		producerMutex.Lock()
		
		// Calculate ideal number of producers based on queue size
		idealProducers := int(queueSize/queueSizeThreshold) + 1 // At least one producer
		
		if idealProducers > maxProducers {
			idealProducers = maxProducers
		} else if idealProducers < minProducers {
			idealProducers = minProducers
		}
		
		// Add producers if needed
		for activeProducers < idealProducers {
			log.Printf("Scaling up: Adding producer #%d (queue size: %d)", activeProducers, queueSize)
			startNewProducer(activeProducers)
			activeProducers++
			lastScaleDown = time.Now() // Reset scale-down timer when scaling up
		}
		
		// Only scale down if queue is small AND we've been idle for a while
		if activeProducers > idealProducers && 
		   activeProducers > minProducers && 
		   time.Since(lastScaleDown) > time.Duration(idleTimeout)*time.Second {
			log.Printf("Scaling down: Removing producer #%d (queue size: %d)", activeProducers-1, queueSize)
			producerControl <- true // Signal a producer to stop
			activeProducers--
			lastScaleDown = time.Now()
		}
		
		producerMutex.Unlock()
		time.Sleep(time.Duration(scaleCheckInterval) * time.Second)
	}
}

// startNewProducer creates a new producer goroutine
func startNewProducer(id int) {
	producerWg.Add(1)
	go func(producerID int) {
		defer producerWg.Done()
		log.Printf("Producer #%d started", producerID)
		
		// Create a context with cancel for this producer
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		
		// Start control channel listener
		go func() {
			select {
			case <-producerControl:
				log.Printf("Producer #%d received stop signal", producerID)
				cancel()
			case <-ctx.Done():
				return
			}
		}()
		
		// Process queries until context is canceled
		processBatchQueries(ctx, producerID)
		
		log.Printf("Producer #%d stopped", producerID)
	}(id)
}

// processBatchQueries fetches queries in batches, executes them, and sends them to Kafka
func processBatchQueries(ctx context.Context, producerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Use a timeout context for batch processing
			batchCtx, cancel := context.WithTimeout(ctx, time.Duration(processingTimeout)*time.Second)
			
			// Get batch of queries using atomic operation with a script
			script := redis.NewScript(`
				local items = redis.call('LRANGE', KEYS[1], 0, ARGV[1])
				if #items > 0 then
					redis.call('LTRIM', KEYS[1], #items, -1)
				end
				return items
			`)
			
			// Execute script to atomically get and remove batch
			result, err := script.Run(batchCtx, redisClient, []string{"query_queue"}, batchSize-1).Result()
			cancel()
			
			if err != nil && err != redis.Nil {
				log.Printf("Producer #%d error fetching batch from Redis: %v", producerID, err)
				time.Sleep(1 * time.Second)
				continue
			}
			
			// Convert result to string slice
			queries, ok := result.([]interface{})
			if !ok || len(queries) == 0 {
				// No items available, wait a bit before checking again
				time.Sleep(1 * time.Second)
				continue
			}
			
			// Convert interface slice to string slice
			queryStrings := make([]string, len(queries))
			for i, q := range queries {
				queryStrings[i] = q.(string)
			}
			
			log.Printf("Producer #%d processing batch of %d queries", producerID, len(queryStrings))
			
			// Execute batch in a transaction
			err = executeBatchQueries(queryStrings)
			if err != nil {
				log.Printf("Producer #%d error executing batch: %v", producerID, err)
				// Here you might implement a retry mechanism or dead-letter queue
				// For simplicity, we'll just log the error
			}
			
			// Publish queries to Kafka
			for _, query := range queryStrings {
				err = publishToKafka("executed-queries", query)
				if err != nil {
					log.Printf("Producer #%d failed to publish query to Kafka: %v", producerID, err)
				}
			}
		}
	}
}

// executeBatchQueries runs a batch of SQL queries in a single transaction
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