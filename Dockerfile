ARG GO_VERSION=1.21
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /usr/src/app
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -v -o /server ./server

FROM debian:bookworm-slim

RUN useradd -m appuser
USER appuser

COPY --from=builder /server /usr/local/bin/
CMD ["server"]
