FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/chronicle ./cmd/collector

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/chronicle /chronicle
EXPOSE 8181
ENTRYPOINT ["/chronicle"]
