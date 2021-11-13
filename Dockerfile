FROM golang:1.17 as builder

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY main.go main.go
COPY db.go db.go
COPY server.go server.go
COPY proto proto

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o app *.go

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/app .
USER 65532:65532

ENTRYPOINT ["/app"]
