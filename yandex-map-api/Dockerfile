# Stage 1: сборка
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Устанавливаем git (нужен для go get)
RUN apk add --no-cache git

# Копируем исходный код
COPY main.go .

# Инициализируем модуль и скачиваем зависимости
RUN go mod init yandex-map-api && \
    go get github.com/joho/godotenv@latest && \
    go get google.golang.org/api/sheets/v4@latest && \
    go get google.golang.org/api/option@latest && \
    go mod tidy

# Собираем бинарник
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o main .

# Stage 2: легковесный образ
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/

COPY --from=builder /app/main .

EXPOSE 8080

CMD ["./main"]