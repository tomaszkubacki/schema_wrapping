package net.tk.transformer;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.HashMap;
import java.util.Map;

public class SchemaWrappingTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String VALUE_MAPPING = "content";
    private static final String VALUE_MAPPING_DEFAULT = "content";
    private static final String KEY_MAPPING = "message_key";
    private static final String KEY_MAPPING_DEFAULT = KEY_MAPPING;
    private static final String TS_MAPPING = "message_ts";
    private static final String TS_MAPPING_DEFAULT = TS_MAPPING;
    private static final String HEADERS_MAPPING = "headers";
    private static final String HEADERS_MAPPING_DEFAULT = "";
    private static final String HEADER_MAPPING_SPLIT_CHAR = ";";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(VALUE_MAPPING, ConfigDef.Type.STRING, VALUE_MAPPING_DEFAULT, ConfigDef.Importance.MEDIUM, "target field name for value if message is schemaless")
            .define(KEY_MAPPING, ConfigDef.Type.STRING, KEY_MAPPING, ConfigDef.Importance.MEDIUM, "target field name for message key")
            .define(TS_MAPPING, ConfigDef.Type.STRING, TS_MAPPING, ConfigDef.Importance.MEDIUM, "target field name to message timestamp")
            .define(HEADERS_MAPPING, ConfigDef.Type.LIST, HEADERS_MAPPING_DEFAULT, ConfigDef.Importance.MEDIUM, "comma separated list of headers mappings to add");

    private static String valueField = VALUE_MAPPING_DEFAULT;
    private static String keyMapping = KEY_MAPPING_DEFAULT;
    private static String tsMapping = TS_MAPPING_DEFAULT;
    private static final Map<String, String> headerMapping = new HashMap<>();

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }
        var headerValues = new HashMap<String, String>();
        headerMapping.keySet().forEach(h -> headerValues.put(headerMapping.get(h), null));
        record.headers().forEach(header -> {
            if (headerMapping.containsKey(header.key())) {
                headerValues.put(headerMapping.get(header.key()), header.value() != null ? header.value().toString() : null);
            }
        });
        var valueSchema = record.valueSchema();

        if (valueSchema == null) {
            return applySchemaless(record, headerValues);
        } else if (record.value() instanceof Struct originalValue) {
            return applyWithSchema(record, originalValue, headerValues);
        }
        return record;
    }

    private static <R extends ConnectRecord<R>> R applyWithSchema(R record, Struct originalValue, HashMap<String, String> headerValues) {
        var valueSchemaBuilder = SchemaUtil.copySchemaBasics(originalValue.schema());

        for (var field : originalValue.schema().fields()) {
            valueSchemaBuilder.field(field.name(), field.schema());
        }

        valueSchemaBuilder.field(keyMapping, Schema.OPTIONAL_STRING_SCHEMA);
        valueSchemaBuilder.field(tsMapping, Schema.INT64_SCHEMA);

        for (var header : headerValues.keySet()) {
            valueSchemaBuilder.field(header, Schema.OPTIONAL_STRING_SCHEMA);
        }

        var updatedValueSchema = valueSchemaBuilder.build();
        var updatedValue = new Struct(updatedValueSchema);

        for (var field : originalValue.schema().fields()) {
            updatedValue.put(field.name(), originalValue.get(field));
        }

        updatedValue.put(KEY_MAPPING, record.key() != null ? record.key().toString() : null);
        updatedValue.put(TS_MAPPING, record.timestamp());

        for (var header : headerValues.keySet()) {
            updatedValue.put(header, headerValues.get(header));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedValueSchema, updatedValue, record.timestamp());
    }

    private static <R extends ConnectRecord<R>> R applySchemaless(R record, HashMap<String, String> headerValues) {
        var valueSchemaBuilder = SchemaBuilder.struct()
                .field(valueField, Schema.OPTIONAL_STRING_SCHEMA)
                .field(keyMapping, Schema.OPTIONAL_STRING_SCHEMA)
                .field(tsMapping, Schema.INT64_SCHEMA);

        for (var header : headerValues.keySet()) {
            valueSchemaBuilder.field(header, Schema.OPTIONAL_STRING_SCHEMA);
        }
        var newValueSchema = valueSchemaBuilder.build();
        var newValue = new Struct(newValueSchema);
        newValue.put(valueField, record.value() != null ? record.value().toString() : null);
        newValue.put(KEY_MAPPING, record.key() != null ? record.key().toString() : null);
        newValue.put(TS_MAPPING, record.timestamp());

        for (var header : headerValues.keySet()) {
            newValue.put(header, headerValues.get(header));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValue.schema(), newValue, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        final var config = new SchemaWrappingConfig(CONFIG_DEF, props);
        valueField = config.getString(VALUE_MAPPING);
        keyMapping = config.getString(KEY_MAPPING);
        tsMapping = config.getString(TS_MAPPING);
        config.getList(HEADERS_MAPPING).forEach(hv -> {
            var headerParts = hv.split(HEADER_MAPPING_SPLIT_CHAR);
            if (headerParts.length > 1) {
                headerMapping.put(headerParts[0], headerParts[1]);
            } else {
                headerMapping.put(headerParts[0], headerParts[0]);
            }
        });
    }
}

