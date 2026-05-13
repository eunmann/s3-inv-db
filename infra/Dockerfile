# Slim production image for s3-inv-db-server.
# Stage 1 builds a static binary; stage 2 runs it from a minimal alpine base
# with a non-root user.

FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
        -o /out/s3-inv-db-server ./cmd/s3-inv-db-server

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget tini && \
    adduser -D -u 1000 -h /home/app app

COPY --from=builder /out/s3-inv-db-server /usr/local/bin/s3-inv-db-server

USER app
WORKDIR /home/app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/sbin/tini", "--", "s3-inv-db-server"]
CMD ["--addr", ":8080"]
