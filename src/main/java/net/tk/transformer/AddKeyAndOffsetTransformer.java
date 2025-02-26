package net.tk.transformer;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class AddKeyAndOffsetTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String OFFSET_KEY = "offset";
    private static final String TOPIC_NAME = "topic_name";

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        // Add offset as key
        Schema keySchema = SchemaBuilder.struct()
                .field(OFFSET_KEY, Schema.INT64_SCHEMA)
                .build();
        Struct newKey = new Struct(keySchema);
        newKey.put(OFFSET_KEY, record.key());

        // Add topic name to value schema
        Schema valueSchema = record.valueSchema();
        if (valueSchema == null) {
            valueSchema = SchemaBuilder.struct()
                    .field(TOPIC_NAME, Schema.STRING_SCHEMA)
                    .build();
            Struct newValue = new Struct(valueSchema);
            newValue.put(TOPIC_NAME, record.topic());
            return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, newKey, valueSchema, newValue, record.timestamp());
        } else {
            Schema updatedValueSchema = SchemaBuilder.array(valueSchema)
                    .field(TOPIC_NAME, Schema.STRING_SCHEMA)
                    .build();
            Struct updatedValue = new Struct(updatedValueSchema);
            updatedValue.put(TOPIC_NAME, record.topic());
            if (record.value() instanceof Struct originalValue) {
                for (var field : originalValue.schema().fields()) {
                    updatedValue.put(field.name(), originalValue.get(field));
                }
            }
            return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, newKey, updatedValueSchema, updatedValue, record.timestamp());
        }
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

