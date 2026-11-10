# Builds on the host's platform and cross-compiles for --platform (no emulation).
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS build
ARG TARGETOS TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /out/chronicle ./cmd/collector \
    && go clean -cache

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/chronicle /chronicle
EXPOSE 8181
ENTRYPOINT ["/chronicle"]
