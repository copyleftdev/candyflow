// File: candyflow-producer/producer.go
package main

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "time"

    "github.com/segmentio/kafka-go"
)

func main() {
    // Kafka writer config
    w := kafka.NewWriter(kafka.WriterConfig{
        Brokers:  []string{"kafka:9092"},
        Topic:    "candy_prices_raw",
        Balancer: &kafka.LeastBytes{},
    })
    defer w.Close()

    log.Println("[Producer] Starting to send messages to candy_prices_raw...")

    rand.Seed(time.Now().UnixNano())

    for i := 0; i < 10; i++ {
        candyID := fmt.Sprintf("Candy%d", rand.Intn(3))   // 3 candy types
        storeID := fmt.Sprintf("Store%d", rand.Intn(5))   // 5 stores
        price := 1.0 + rand.Float64()*5.0                 // random price between 1.0 and 6.0

        msgValue := fmt.Sprintf(`{"candy_id":"%s","store_id":"%s","price":%.2f,"timestamp":%d}`,
            candyID, storeID, price, time.Now().Unix())

        log.Printf("[Producer] Sending message: %s\n", msgValue)
        err := w.WriteMessages(context.Background(), kafka.Message{
            Key:   []byte(fmt.Sprintf("%s:%s", candyID, storeID)),
            Value: []byte(msgValue),
        })
        if err != nil {
            log.Printf("[Producer] Failed to write message: %v\n", err)
        }

        time.Sleep(1 * time.Second)
    }

    log.Println("[Producer] Finished sending messages.")
}
