package net.tk.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SelectTopicTransformTest {

    SelectTopicTransform<SourceRecord> transformer = new SelectTopicTransform<>();

    @Test
    void selectTopic() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic_field", "kafka_topic");
        transformer.configure(props);

        final var testSchema = SchemaBuilder.struct()
                .name("name")
                .version(1).doc("doc")
                .field("roko", Schema.OPTIONAL_INT64_SCHEMA)
                .field("kafka_topic", Schema.STRING_SCHEMA)
                .build();

        final var testStruct = new Struct(testSchema)
                .put("roko", 42L)
                .put("kafka_topic", "kafka_topic_new");

        final SourceRecord record = createStructRecord(testSchema, testStruct);
        final SourceRecord transformedRecord = transformer.apply(record);

        Struct value = (Struct) transformedRecord.value();
        assertEquals(42L, value.get("roko"));
        assertEquals("kafka_topic_new", transformedRecord.topic());
    }


    static SourceRecord createStructRecord(Schema schema,
                                           Struct struct) {
        return new SourceRecord(null, null, "test", 0,
                null, null, schema, struct, 0L, null);
    }


}