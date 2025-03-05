# Schema Wrapping

This project contains [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) facilitating 
classes allowing to stream a schemaless data into the sink requiring schema e.g. 
[JdbcSinkConnector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)

### Use SimpleSchemaWrappingConverter
In order to put raw schemaless messages into sql table use *SimpleSchemaWrappingConverter* 
```java
import net.tk.kafka.connect.converter.SimpleSchemaWrappingConverter;
```
and (optional) *AddMetadataTransform* if you want to have topic key, timestamp and headers in your table 
```java
import net.tk.kafka.connect.transforms.AddMetadataTransform;
```
### Example configuration for PostgreSQL

```json
{
  "name": "kafka_to_postgres",
  "config": {
    "topics": "kafka_sink",
    "input.data.format": "JSON",
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "dialect.name": "PostgreSqlDatabaseDialect",
    "connection.url": "jdbc:postgresql://pg:5432/my_db",
    "connection.user": "docker",
    "connection.password": "docker",
    "connection.backoff.ms": "5000",
    "db.timezone": "UTC",
    "auto.create": "true",
    "auto.evolve": "false",
    "tasks.max": "1",
    "batch.size": "1000",
    "insert.mode": "INSERT",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "key.converter.schemas.enable": "false",
    "value.converter": "net.tk.kafka.connect.converter.SimpleSchemaWrappingConverter",
    "value.converter.schemas.enable": "false",
    "transforms": "addRecordMetadata",
    "transforms.addRecordMetadata.type": "net.tk.kafka.connect.transforms.AddMetadataTransform",
    "transforms.addRecordMetadata.headers": "a-b-c;abc,other_header;other_header_mapped",
    "pk.mode": "kafka",
    "pk.fields": "kafka_topic,kafka_partition,kafka_offset"
  }
}
```

### Configurations

Both *SimpleSchemaWrappingConverter* and *AddMetadataTransform* allow config field mappings (e.g. to change column name in JdbcSinkConnector).
