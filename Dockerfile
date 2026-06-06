# syntax=docker/dockerfile:1

# --- build stage ---
FROM golang:1.25-alpine AS build
WORKDIR /src

# Fetch dependencies first so this layer is cached across source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY web/ web/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/cidindexer-ipfs .

# --- runtime stage ---
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -u 10001 app \
    && mkdir -p /data \
    && chown app:app /data

COPY --from=build /out/cidindexer-ipfs /usr/local/bin/cidindexer-ipfs

USER app
WORKDIR /data
EXPOSE 8384
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8384/api/stats >/dev/null 2>&1 || exit 1

ENTRYPOINT ["cidindexer-ipfs"]
# Persist the index/archives/moderation state under the mounted /data volume.
CMD ["-o", "/data"]
