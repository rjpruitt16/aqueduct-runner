FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /websocket-fixture .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /websocket-fixture /websocket-fixture
EXPOSE 6060
CMD ["/websocket-fixture", "server"]
