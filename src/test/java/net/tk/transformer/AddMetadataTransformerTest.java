package net.tk.transformer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class AddMetadataTransformerTest {

    AddMetadataTransformer<SourceRecord> transformer = new AddMetadataTransformer<>();

    @Test
    void apply() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        transformer.configure(props);

        final var simpleStructSchema = SchemaBuilder.struct()
                .name("name")
                .version(1).doc("doc")
                .field("roko", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final var simpleStruct = new Struct(simpleStructSchema).put("roko", 42L);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, "zonk", simpleStructSchema, simpleStruct, 789L);
        final SourceRecord transformedRecord = transformer.apply(record);
        var valueSchemaFields = transformedRecord.valueSchema().fields();
        assert valueSchemaFields.stream().anyMatch(field -> field.name().equals("message_key"));
        assert valueSchemaFields.stream().anyMatch(field -> field.name().equals("message_ts"));
    }



}