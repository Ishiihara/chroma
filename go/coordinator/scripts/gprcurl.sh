grpcurl -import-path internal/proto -proto worker.proto -plaintext localhost:7070  workerpb.WorkerService/CheckHealthgrpcurl -import-path internal/proto -proto coordinator.proto -plaintext localhost:7070 list

grpcurl -d @ -import-path internal/proto -proto coordinator.proto -plaintext localhost:7070 coordinator.MetadataService/CreateCollection <<EOM
{
  "collection": {
    "id": "3e526d4b-0841-4740-82a7-52a62485d530",
    "name": "test",
    "metadata": {
      "metadata": {"key1": {"string_value":"value1"}}
    }
  }
}
EOM
