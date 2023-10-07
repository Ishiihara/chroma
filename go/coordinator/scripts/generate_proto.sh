#!/usr/bin/env bash

protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative worker.proto
protoc -I. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative coordinator.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative logwriter.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative metadata.proto

grpcurl -import-path internal/proto -proto worker.proto -plaintext localhost:7070  workerpb.WorkerService/CheckHealth

grpcurl -d '{"term":0}' -import-path internal/proto -proto logwriter.proto -plaintext localhost:9090 logwriterpb.LogWriterService/GetLogWriterAssignment

export GOPATH=~/go

protoc --go_out=./coordinatorpb --go_opt paths=source_relative --plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" --go-grpc_out=./coordinatorpb --go-grpc_opt paths=source_relative --plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" coordinator.proto -I.
protoc --go_out=./metadatapb --go_opt paths=source_relative --plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" --go-grpc_out=./metadatapb --go-grpc_opt paths=source_relative --plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" metadata.proto -I.
protoc --go_out=./commonpb --go_opt paths=source_relative --plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" --go-grpc_out=./commonpb --go-grpc_opt paths=source_relative --plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" common.proto -I.


protoc --go_out=../../go/coordinator/internal/proto/coordinatorpb --go_opt paths=source_relative --plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" --go-grpc_out=../../go/coordinator/internal/proto/coordinatorpb --go-grpc_opt paths=source_relative --plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" chroma.proto -I.