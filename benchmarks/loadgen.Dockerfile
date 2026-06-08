FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.mod
COPY benchmarks benchmarks

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/loadgen ./benchmarks/cmd/loadgen

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=build /out/loadgen /usr/local/bin/loadgen
COPY benchmarks/scripts/loadgen-shard-entrypoint.sh /usr/local/bin/loadgen-shard-entrypoint.sh

RUN chmod +x /usr/local/bin/loadgen-shard-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/loadgen-shard-entrypoint.sh"]
