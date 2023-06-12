// main.go

package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var rdb *redis.Client

type Message struct {
	ID        int       `json:"id"`
	Content   string    `json:"content"`
	Sender    string    `json:"sender"`
	Recipient string    `json:"recipient"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	// Initialize database connection
	db = initDB()
	defer db.Close()

	// Initialize Redis client
	rdb = initRedisClient()
	defer rdb.Close()

	// Create HTTP server
	router := mux.NewRouter()
	router.HandleFunc("/messages", getMessages).Methods("GET")

	log.Fatal(http.ListenAndServe(":8000", router))
}

func initDB() *sql.DB {
	// Establish database connection
	db, err := sql.Open("mysql", "username:password@tcp(localhost:3306)/messages_db")
	if err != nil {
		log.Fatal(err)
	}

	// Ping database to ensure connection is established
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func initRedisClient() *redis.Client {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Ping Redis to ensure connection is established
	_, err := rdb.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	return rdb
}

func getMessages(w http.ResponseWriter, r *http.Request) {
	recipient := r.URL.Query().Get("recipient")

	// Check Redis cache for messages
	cacheKey := "messages:" + recipient
	result, err := rdb.Get(cacheKey).Result()
	if err == nil {
		// Cache hit, return messages from Redis
		var messages []Message
		err = json.Unmarshal([]byte(result), &messages)
		if err != nil {
			log.Println("Error unmarshaling messages from cache:", err)
		}
		json.NewEncoder(w).Encode(messages)
		return
	}

	// Cache miss, fetch messages from the database
	query := "SELECT id, content, sender, recipient, created_at FROM messages WHERE recipient = ?"
	rows, err := db.Query(query, recipient)
	if err != nil {
		log.Println("Error querying database:", err)
		http.Error(w, "Failed to fetch messages", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var messages []Message

	// Iterate through rows and build messages slice
	for rows.Next() {
		var msg Message
		err = rows.Scan(&msg.ID, &msg.Content, &msg.Sender, &msg.Recipient, &msg.CreatedAt)
		if err != nil {
			log.Println("Error scanning row:", err)
			continue
		}
		messages = append(messages, msg)
	}

	// Store messages in Redis cache
	cacheValue, err := json.Marshal(messages)
	if err == nil {
		err = rdb.Set(cacheKey, cacheValue, 24*time.Hour).Err()
		if err != nil {
			log.Println("Error storing messages in cache:", err)
		}
	}

	json.NewEncoder(w).Encode(messages)
}
