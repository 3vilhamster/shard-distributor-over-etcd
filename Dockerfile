FROM golang:1.24

WORKDIR /app
COPY . .

RUN go mod download
RUN go build -o shard-distributor cmd/poc/poc.go

ENTRYPOINT ["./shard-distributor"]
