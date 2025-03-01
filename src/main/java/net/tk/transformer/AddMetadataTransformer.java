package net.tk.transformer;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class AddMetadataTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String KEY_MAPPING = "message_key";
    private static final String KEY_MAPPING_DEFAULT = KEY_MAPPING;
    private static final String TS_MAPPING = "message_ts";
    private static final String TS_MAPPING_DEFAULT = TS_MAPPING;
    private static final String HEADERS_REGEX = "headers_regex";
    private static final String HEADERS_REGEX_DEFAULT = "";
    private static final String HEADERS_PREFIX = "headers_prefix";
    private static final String HEADERS_PREFIX_DEFAULT = "header_";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_MAPPING, ConfigDef.Type.STRING, KEY_MAPPING, ConfigDef.Importance.MEDIUM, "target field name for message key")
            .define(TS_MAPPING, ConfigDef.Type.STRING, TS_MAPPING, ConfigDef.Importance.MEDIUM, "target field name to message timestamp")
            .define(HEADERS_REGEX, ConfigDef.Type.LIST, HEADERS_REGEX_DEFAULT, ConfigDef.Importance.MEDIUM, "regex selecting headers to map")
            .define(HEADERS_PREFIX, ConfigDef.Type.STRING, HEADERS_PREFIX_DEFAULT, ConfigDef.Importance.LOW, "prefix for target header field names");

    private static String keyMapping = KEY_MAPPING_DEFAULT;
    private static String tsMapping = TS_MAPPING_DEFAULT;
    private static String headersRegex = HEADERS_REGEX_DEFAULT;
    // TODO define regex
    // private static Math headersMatch = new Match;

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        // TODO - add regex detected header fields

        record.headers().forEach(header -> {
             // TODO add matching headers to field list
        });

        var newValueSchema = SchemaBuilder.struct()
                .field(keyMapping, Schema.OPTIONAL_STRING_SCHEMA)
                .field(tsMapping, Schema.INT64_SCHEMA)
                .build();

        var valueSchema = record.valueSchema();
        if (valueSchema == null) {
            var newValue = new Struct(newValueSchema);
            newValue.put(KEY_MAPPING, record.key() != null ? record.key().toString() : null);
            newValue.put(TS_MAPPING, record.timestamp());
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValue.schema(), newValue, record.timestamp());
        } else {
        // TODO - add values to existing struct


//            if (record.value() instanceof Struct originalValue) {
//                originalValue.
//            }
//
//            Schema updatedSchema = schemaUpdateCache.get(schema);
//            if (updatedSchema == null) {
//                updatedSchema = SchemaBuilder.struct().field(fieldName, schema).build();
//                schemaUpdateCache.put(schema, updatedSchema);
//            }
//
//            final Struct updatedValue = new Struct(updatedSchema).put(fieldName, value);
//
//            return newRecord(record, updatedSchema, updatedValue);
//
//            return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, newKey, newValueSchema, updatedValue, record.timestamp());
            return null;
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
    public void configure(Map<String, ?> props) {
        final var config = new AddMetadataConfig(CONFIG_DEF, props);
        keyMapping = config.getString(KEY_MAPPING);
        tsMapping = config.getString(TS_MAPPING);
        headersRegex = config.getString(HEADERS_REGEX);
    }
}

