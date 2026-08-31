FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build all three
RUN CGO_ENABLED=0 go build -o /out/frontend ./victim/frontend
RUN CGO_ENABLED=0 go build -o /out/api ./victim/api
RUN CGO_ENABLED=0 go build -o /out/worker ./victim/worker

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/frontend /frontend
COPY --from=builder /out/api /api
COPY --from=builder /out/worker /worker

# Default to frontend, override in kubernetes manifest
ENTRYPOINT ["/frontend"]
