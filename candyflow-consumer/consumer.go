// File: candyflow-consumer/consumer.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Initialize our Arrow data holder
	arrowData := NewArrowData()

	// Start consuming from Kafka in a separate goroutine
	go func() {
		consumeKafka(arrowData)
	}()

	// Expose a simple HTTP API
	http.HandleFunc("/cheapest", func(w http.ResponseWriter, r *http.Request) {
		candyID := r.URL.Query().Get("candy_id")
		if candyID == "" {
			http.Error(w, "Missing candy_id param", http.StatusBadRequest)
			return
		}
		entry, ok := arrowData.GetCheapest(candyID)
		if !ok {
			http.Error(w, "No data found for candy_id="+candyID, http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "Cheapest %s is at store %s with price %.2f\n", candyID, entry.StoreID, entry.Price)
	})

	http.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		arrowData.DebugPrint()
		w.Write([]byte("Debug info printed to console.\n"))
	})

	http.HandleFunc("/rows", func(w http.ResponseWriter, r *http.Request) {
		count := arrowData.GetRecordCount()
		fmt.Fprintf(w, "Current row count in Arrow: %d\n", count)
	})

	log.Println("[Consumer] Starting HTTP server on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func consumeKafka(arrowData *ArrowData) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		Topic:     "candy_prices_raw",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()

	log.Println("[Consumer] Subscribed to candy_prices_raw. Waiting for messages...")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[Consumer] Error reading message: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var cp CandyPrice
		if err := json.Unmarshal(m.Value, &cp); err != nil {
			log.Printf("[Consumer] JSON unmarshal error: %v\n", err)
			continue
		}
		arrowData.AddCandyPrice(cp)

		log.Printf("[Consumer] Received: %s -> CandyID=%s StoreID=%s Price=%.2f",
			string(m.Value), cp.CandyID, cp.StoreID, cp.Price)
	}
}
