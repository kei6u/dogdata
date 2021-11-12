FROM golang:1.17 as builder

WORKDIR /workspace
COPY /api/go.mod go.mod
COPY /api/go.sum go.sum
RUN go mod download

COPY /api/main.go main.go
COPY /api/db.go db.go
COPY /api/server.go server.go
COPY /api/proto proto

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o app *.go

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/app .
USER 65532:65532

ENTRYPOINT ["/app"]
