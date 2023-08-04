# Kafka Streams app to validate schemas


## Manual tests

```shell
#produce message without magic byte:
 user_schema='{
  "type": "record",
  "name": "User",
  "fields": [
      {"name": "name", "type": "string" },
      {"name": "id", "type": "int" }
  ]
}'

kafka-console-producer --bootstrap-server localhost:9092 --topic input


kafka-avro-console-producer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 --property value.schema="$user_schema" \
--topic input

{"name": "Christoph", "id": 1}

```


## ToDo
- [ ] chaos testing: what happens when SR is not available?