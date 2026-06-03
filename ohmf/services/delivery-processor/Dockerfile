FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -o /bin/delivery-processor ./cmd/processor

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/delivery-processor /bin/delivery-processor
EXPOSE 18089
ENTRYPOINT ["/bin/delivery-processor"]
