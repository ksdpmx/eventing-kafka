FROM --platform=${BUILDPLATFORM} docker.io/golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
  go build -o dispatcher -a -trimpath -ldflags '-extldflags "-static"' ./cmd/channel/distributed/dispatcher/main.go

FROM gcr.io/distroless/static-debian12:debug

COPY --from=builder /workspace/dispatcher /usr/local/bin/dispatcher
ENTRYPOINT ["/usr/local/bin/dispatcher"]
