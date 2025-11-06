FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

WORKDIR /app
COPY . .

WORKDIR /app/telemetry/server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o telemetry-server .

FROM alpine:latest
RUN apk --no-cache add ca-certificates \
    && addgroup -S appgroup \
    && adduser -S appuser -G appgroup

WORKDIR /home/appuser/
COPY --from=builder /app/telemetry/server/telemetry-server .

EXPOSE 8080

USER appuser

CMD ["./telemetry-server"]