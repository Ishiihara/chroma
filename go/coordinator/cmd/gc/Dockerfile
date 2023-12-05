FROM golang:1.20-alpine as builder

WORKDIR /app

RUN apk add --no-cache make git build-base bash

# copy modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
COPY internal internal

# cache modules
# RUN go mod download

# copy source code
COPY cmd/gc/main.go main.go

# build
RUN go build -v -o gc-processor main.go

FROM alpine:3.17.3

RUN apk add --no-cache bash bash-completion

RUN apk --no-cache add ca-certificates

USER nobody

COPY --from=builder --chown=nobody:nobody /app/gc-processor .

ENTRYPOINT ["./gc-processor"]