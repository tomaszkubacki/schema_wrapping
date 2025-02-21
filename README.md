## Schema Wrapping

Use SimpleSchemaWrappingConverter
```
net.tk.converter.SimpleSchemaWrappingConverter
```
as input data converter and wrap it into struct with a single field 
*content*.

This will allow use any text based topic with e.g. JdbcSinkConnector
