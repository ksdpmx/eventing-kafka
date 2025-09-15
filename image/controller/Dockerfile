FROM --platform=${BUILDPLATFORM} docker.io/golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
  go build -o controller -a -trimpath -ldflags '-extldflags "-static"' ./cmd/channel/distributed/controller/main.go

FROM gcr.io/distroless/static-debian12:debug

COPY --from=builder /workspace/controller /usr/local/bin/controller
ENTRYPOINT ["/usr/local/bin/controller"]
