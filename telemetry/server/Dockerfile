FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY . .

WORKDIR /app/telemetry/server
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o telemetry-server .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/telemetry/server/telemetry-server .

EXPOSE 8080

CMD ["./telemetry-server"] 