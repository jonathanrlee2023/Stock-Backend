# STAGE 1: Build the binary
FROM golang:1.26-alpine AS builder

# Install build dependencies to ensure everything is fresh
RUN apk add --no-cache git tzdata
WORKDIR /app

# Copy dependency files
COPY Go_Backend/go.mod Go_Backend/go.sum ./
RUN go mod download

# Copy source and build
COPY Go_Backend/ . 
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o server .

# STAGE 2: Distroless for Zero Vulnerabilities
# This contains ONLY the CA certificates and the timezone data
FROM gcr.io/distroless/static-debian12:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/server .

# Copy timezone data (crucial for stock market apps)
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Run as non-root user (Security Best Practice)
USER nonroot:nonroot

CMD ["./server"]