# Schema Wrapping

This project contains [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) facilitating 
classes allowing to stream a schemaless data into the sink requiring schema e.g. 
[JdbcSinkConnector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)

### Use SimpleSchemaWrappingConverter
In order to put raw schemaless content into sql table use 
```
net.tk.converter.SimpleSchemaWrappingConverter
```
as *value.converter* e.g. following is a full JdbcSinkConnector configuration using *SimpleSchemaWrappingConverter*

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
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "insert.mode": "INSERT",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "pk.mode": "none"
  }
}
```