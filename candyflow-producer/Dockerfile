# File: candyflow-producer/Dockerfile
FROM golang:1.20-alpine AS builder
WORKDIR /app

COPY producer.go .
RUN go mod init candyflow-producer && \
    go get github.com/segmentio/kafka-go && \
    go build -o producer producer.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/producer .
CMD ["./producer"]
