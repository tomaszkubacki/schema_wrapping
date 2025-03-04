package net.tk.transformer;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AddMetadataTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String KEY_MAPPING = "message_key";
    private static final String KEY_MAPPING_DEFAULT = KEY_MAPPING;
    private static final String TS_MAPPING = "message_ts";
    private static final String TS_MAPPING_DEFAULT = TS_MAPPING;
    private static final String HEADERS = "headers";
    private static final String HEADERS_REGEX_DEFAULT = "";
    private static final String HEADER_PREFIX = "header_prefix";
    private static final String HEADER_PREFIX_DEFAULT = "hdr_";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_MAPPING, ConfigDef.Type.STRING, KEY_MAPPING, ConfigDef.Importance.MEDIUM, "target field name for message key")
            .define(TS_MAPPING, ConfigDef.Type.STRING, TS_MAPPING, ConfigDef.Importance.MEDIUM, "target field name to message timestamp")
            .define(HEADERS, ConfigDef.Type.LIST, HEADERS_REGEX_DEFAULT, ConfigDef.Importance.MEDIUM, "comma separated list of headers to add")
            .define(HEADER_PREFIX, ConfigDef.Type.STRING, HEADER_PREFIX_DEFAULT, ConfigDef.Importance.LOW, "prefix for target header field names");

    private static String keyMapping = KEY_MAPPING_DEFAULT;
    private static String tsMapping = TS_MAPPING_DEFAULT;
    private static String headerPrefix = HEADER_PREFIX_DEFAULT;
    private static final Set<String> headerSet = new HashSet<>();

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var headerValues = new HashMap<String, String>();
        headerSet.forEach(h -> headerValues.put(h, null));
        record.headers().forEach(header -> {
            if (headerSet.contains(header.key())) {
                headerValues.put(header.key(), header.value() != null ? header.value().toString() : null);
            }
        });
        var valueSchema = record.valueSchema();

        if (valueSchema == null) {
            var valueSchemaBuilder = SchemaBuilder.struct()
                    .field(keyMapping, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(tsMapping, Schema.INT64_SCHEMA);

            for (var header : headerValues.keySet()) {
                valueSchemaBuilder.field(headerPrefix + header, Schema.OPTIONAL_STRING_SCHEMA);
            }
            var newValueSchema = valueSchemaBuilder.build();
            var newValue = new Struct(newValueSchema);
            newValue.put(KEY_MAPPING, record.key() != null ? record.key().toString() : null);
            newValue.put(TS_MAPPING, record.timestamp());

            for (var header : headerValues.keySet()) {
                newValue.put(headerPrefix + header, headerValues.get(header));
            }

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValue.schema(), newValue, record.timestamp());
        } else if (record.value() instanceof Struct originalValue) {
            var valueSchemaBuilder = SchemaUtil.copySchemaBasics(originalValue.schema());

            for (var field : originalValue.schema().fields()) {
                valueSchemaBuilder.field(field.name(), field.schema());
            }

            valueSchemaBuilder.field(keyMapping, Schema.OPTIONAL_STRING_SCHEMA);
            valueSchemaBuilder.field(tsMapping, Schema.INT64_SCHEMA);

            for (var header : headerValues.keySet()) {
                valueSchemaBuilder.field(headerPrefix + header, Schema.OPTIONAL_STRING_SCHEMA);
            }

            var updatedValueSchema = valueSchemaBuilder.build();
            var updatedValue = new Struct(updatedValueSchema);

            for (var field : originalValue.schema().fields()) {
                updatedValue.put(field.name(), originalValue.get(field));
            }

            updatedValue.put(KEY_MAPPING, record.key() != null ? record.key().toString() : null);
            updatedValue.put(TS_MAPPING, record.timestamp());

            for (var header : headerValues.keySet()) {
                updatedValue.put(headerPrefix + header, headerValues.get(header));
            }

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedValueSchema, updatedValue, record.timestamp());
        }
        return record;
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
        final var config = new AddMetadataConfig(CONFIG_DEF, props);
        keyMapping = config.getString(KEY_MAPPING);
        tsMapping = config.getString(TS_MAPPING);
        headerPrefix = config.getString(HEADER_PREFIX);
        headerSet.addAll(config.getList(HEADERS));
    }
}

