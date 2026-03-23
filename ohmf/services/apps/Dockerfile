FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY ohmf ./ohmf
COPY services ./services
COPY pkg ./pkg
RUN go build -o /bin/apps ./services/apps

FROM alpine:latest
COPY --from=build /bin/apps /bin/apps
COPY --from=build /src/services/apps/migrations /opt/ohmf/apps/migrations
EXPOSE 18086
CMD ["/bin/apps"]
