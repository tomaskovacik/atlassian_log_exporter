# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o atlassian_log_exporter .

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/atlassian_log_exporter /usr/local/bin/atlassian_log_exporter

ENTRYPOINT ["/usr/local/bin/atlassian_log_exporter"]
