FROM golang:latest as builder
WORKDIR /statsdaemon/
COPY . .
RUN CGO_ENABLED=0 go build -o statsdaemon

FROM alpine:latest
COPY --from=builder /statsdaemon/statsdaemon /usr/local/bin/statsdaemon
ENTRYPOINT ["statsdaemon"]
