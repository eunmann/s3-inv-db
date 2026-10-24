# Slim production image for s3-inv-db-server.
# Stage 1 builds a static binary; stage 2 runs it from a minimal alpine base
# with a non-root user.

# The builder runs on the build host's platform and cross-compiles, so a
# multi-arch build never compiles Go under emulation.
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS builder

# make + curl for the Tailwind CLI download in `make css`.
RUN apk add --no-cache make curl

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Regenerate Tailwind CSS so the embedded stylesheet matches the
# templates in this image (not whatever was checked into the repo at
# the time of clone). This guarantees the prod image always serves an
# in-sync compiled stylesheet.
RUN make css

ARG TARGETOS=linux TARGETARCH
# Version metadata for --version (internal/buildinfo). The build context
# has no .git, so unset values report as dev/unknown.
ARG VERSION COMMIT DATE
RUN BUILDINFO=github.com/eunmann/s3-inv-db/internal/buildinfo && \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -trimpath \
        -ldflags="-s -w -X $BUILDINFO.version=$VERSION -X $BUILDINFO.commit=$COMMIT -X $BUILDINFO.date=$DATE" \
        -o /out/s3-inv-db-server ./cmd/s3-inv-db-server

FROM alpine:3.21

# The default --cache-dir must be writable by the non-root user, and a
# named volume mounted there inherits this ownership.
RUN apk add --no-cache ca-certificates wget tini && \
    adduser -D -u 1000 -h /home/app app && \
    install -d -o app -g app /var/cache/s3inv

COPY --from=builder /out/s3-inv-db-server /usr/local/bin/s3-inv-db-server

USER app
WORKDIR /home/app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/sbin/tini", "--", "s3-inv-db-server"]
CMD ["--addr", ":8080"]
