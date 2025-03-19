FROM golang:1.24

WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Then copy the rest of the application
COPY . .

RUN go build -o shard-distributor cmd/poc/poc.go

ENTRYPOINT ["./shard-distributor"]
