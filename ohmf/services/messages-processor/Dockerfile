FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -o /bin/messages-processor ./cmd/processor

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/messages-processor /bin/messages-processor
EXPOSE 18088
ENTRYPOINT ["/bin/messages-processor"]
