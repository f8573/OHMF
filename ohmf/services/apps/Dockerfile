FROM golang:1.25-alpine AS build
WORKDIR /src/ohmf
COPY ohmf /src/ohmf
RUN go build -o /bin/apps ./services/apps

FROM alpine:latest
COPY --from=build /bin/apps /bin/apps
COPY --from=build /src/ohmf/services/apps/migrations /opt/ohmf/apps/migrations
EXPOSE 18086
CMD ["/bin/apps"]
