FROM golang:1.23-bullseye AS builder

WORKDIR /root
RUN git clone --branch release-1.3 https://github.com/ksdpmx/eventing-kafka && \
    cd eventing-kafka && \
    go mod tidy && \
    go mod vendor && \
    CGO_ENABLED=0 go build -o controller -a -ldflags '-extldflags "-static"' ./cmd/channel/distributed/controller/main.go

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /root/eventing-kafka/controller /usr/local/bin/controller
ENTRYPOINT ["/usr/local/bin/controller"]
