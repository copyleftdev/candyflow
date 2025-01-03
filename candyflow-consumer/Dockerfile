# File: candyflow-consumer/Dockerfile
FROM golang:1.20-alpine AS builder
WORKDIR /app

# Copy files
COPY consumer.go .
COPY arrow_data.go .

# Initialize modules and install dependencies
RUN go mod init candyflow-consumer && \
    go get github.com/segmentio/kafka-go && \
    go get github.com/apache/arrow/go/arrow/... && \
    # Compile both .go files, so we pick up symbols in arrow_data.go
    go build -o consumer *.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/consumer .
CMD ["./consumer"]
