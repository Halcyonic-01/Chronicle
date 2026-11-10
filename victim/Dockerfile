# Builds on the host's platform and cross-compiles for --platform (no emulation).
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS builder
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build all three
RUN go build -o /out/frontend ./victim/frontend
RUN go build -o /out/api ./victim/api
RUN go build -o /out/worker ./victim/worker
RUN go clean -cache

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/frontend /frontend
COPY --from=builder /out/api /api
COPY --from=builder /out/worker /worker

# Default to frontend, override in kubernetes manifest
ENTRYPOINT ["/frontend"]
