FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -o /bin/api ./cmd/api
RUN go build -o /bin/worker ./cmd/worker

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /bin/api /bin/api
COPY --from=builder /bin/worker /bin/worker
COPY migrations ./migrations
EXPOSE 8080
ENTRYPOINT ["/bin/api"]
