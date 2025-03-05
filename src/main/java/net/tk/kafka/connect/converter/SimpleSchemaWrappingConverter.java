package net.tk.kafka.connect.converter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

import static net.tk.kafka.connect.converter.SimpleSchemaWrappingConverterConfig.CONTENT_NAME_DEFAULT;


public class SimpleSchemaWrappingConverter implements Converter, Versioned {
    public static String CONTENT = CONTENT_NAME_DEFAULT;
    private final StringSerializer serializer = new StringSerializer();
    private final StringDeserializer deserializer = new StringDeserializer();

    private static Schema SCHEMA = null;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public ConfigDef config() {
        return SimpleSchemaWrappingConverterConfig.configDef();
    }

    public void configure(Map<String, ?> configs) {
        var conf = new SimpleSchemaWrappingConverterConfig(configs);
        var encoding = conf.encoding();
        CONTENT = conf.contentName();
        SCHEMA = SchemaBuilder.struct()
                .field(CONTENT, Schema.STRING_SCHEMA)
                .build();
        var serializerConfigs = new HashMap<String, Object>(configs);
        var deserializerConfigs = new HashMap<String, Object>(configs);
        serializerConfigs.put("serializer.encoding", encoding);
        deserializerConfigs.put("deserializer.encoding", encoding);
        boolean isKey = conf.type() == ConverterType.KEY;
        serializer.configure(serializerConfigs, isKey);
        deserializer.configure(deserializerConfigs, isKey);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        var conf = new HashMap<String, Object>(configs);
        conf.put(SimpleSchemaWrappingConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            if (value instanceof Struct struct) {
                String strValue = struct.getString(CONTENT);
                return serializer.serialize(topic, strValue);
            }
            return null;
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to a string: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            var strValue = deserializer.deserialize(topic, value);
            var struct = new Struct(SCHEMA);
            struct.put(CONTENT, strValue);
            return new SchemaAndValue(SCHEMA, struct);
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize string: ", e);
        }
    }

}
