package net.tk.transformer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AddMetadataTransformerTest {

    AddMetadataTransformer<SourceRecord> transformer = new AddMetadataTransformer<>();

    @Test
    void apply() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header_prefix", "");
        props.put("headers", "ce_id,abc");

        transformer.configure(props);

        final var simpleStructSchema = SchemaBuilder.struct()
                .name("name")
                .version(1).doc("doc")
                .field("roko", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final var simpleStruct = new Struct(simpleStructSchema).put("roko", 42L);

        final Map<String, String> headersMap = new HashMap<>();
        headersMap.put("ce_id", "2342343242");
        headersMap.put("abc", "zoo");
        var ts = 83883L;
        final SourceRecord record = createRecord("zonk", ts, simpleStructSchema, simpleStruct, headersMap);
        final SourceRecord transformedRecord = transformer.apply(record);
        var valueSchema = transformedRecord.valueSchema();
        assertNotNull(valueSchema.field("message_key"));
        assertNotNull(valueSchema.field("message_ts"));
        assertNotNull(valueSchema.field("ce_id"));
        assertNotNull(valueSchema.field("abc"));
        assertNotNull(valueSchema.field("roko"));
        Struct value = (Struct) transformedRecord.value();
        assertEquals("zonk", value.get("message_key"));
        assertEquals(ts, value.get("message_ts"));
        assertEquals("zoo", value.get("abc"));
        assertEquals(42L, value.get("roko"));
    }

    static SourceRecord createRecord(String key, long ts, Schema simpleStructSchema,
                                     Struct simpleStruct, Map<String, String> headersMap) {
        return new SourceRecord(null, null, "test", 0,
                null, key, simpleStructSchema,
                simpleStruct, ts, createHeaderList(headersMap));
    }

    static List<Header> createHeaderList(Map<String, String> headersMap) {
        return headersMap.keySet().stream().map(h -> (Header) new ConnectTestHeader(h, headersMap.get(h))).toList();
    }

}

class ConnectTestHeader implements Header {

    final String key;
    final Schema schema;
    final String value;

    public ConnectTestHeader(String key, Schema schema, String value) {
        this.key = key;
        this.schema = schema;
        this.value = value;
    }

    public ConnectTestHeader(String key, String value) {
        this(key, Schema.STRING_SCHEMA, value);
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public Header with(Schema schema, Object value) {
        return new ConnectTestHeader(this.key, this.schema, this.value);
    }

    @Override
    public Header rename(String key) {
        return new ConnectTestHeader(key, this.schema, this.value);
    }
}