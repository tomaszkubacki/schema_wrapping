package net.tk.kafka.connect.transforms;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class SelectTopicTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String TOPIC_FIELD = "topic_field";
    private static final String TOPIC_FIELD_DEFAULT = "";
    private static String topicField = TOPIC_FIELD_DEFAULT;


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_FIELD, STRING, TOPIC_FIELD_DEFAULT, HIGH, "topic field name from which topic name will be taken");


    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        if (record.value() instanceof Struct structValue) {
            var topic = structValue.get(topicField).toString();
            return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
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
        topicField = config.getString(TOPIC_FIELD);
    }
}

