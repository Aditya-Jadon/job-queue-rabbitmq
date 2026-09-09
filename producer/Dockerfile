FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o producer .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/producer .

CMD ["./producer"]