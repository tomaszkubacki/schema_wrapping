package net.tk.kafka.connect.converter;


import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


class SimpleSchemaWrappingConverterTest {

    private static final String TOPIC = "topic1";
    private static final String CONTENT = "my message content";
    private final SimpleSchemaWrappingConverter converter = new SimpleSchemaWrappingConverter();

    @BeforeEach
    public void setUp() {
        converter.configure(Collections.<String, String>emptyMap(), false);
    }

    @Test
    void toConnectData() {
        var res = converter.toConnectData(TOPIC, CONTENT.getBytes(StandardCharsets.UTF_8));
        var schema = res.schema();
        assertNotNull(schema.field("content"));
        assertNull(schema.field("bla bla"));
        var value = (Struct) res.value();
        assertNotNull(value);
    }

    @Test
    void toConnectDataNonDefaultContentName() {
        var contentFieldName = "otherContent";
        var config = new HashMap<String,Object>();
        config.put("content.name", contentFieldName);
        converter.configure(config, false);
        var res = converter.toConnectData(TOPIC, CONTENT.getBytes(StandardCharsets.UTF_8));
        var schema = res.schema();
        assertNotNull(schema.field(contentFieldName));
        assertNull(schema.field("bla bla"));
        var value = (Struct) res.value();
        assertNotNull(value);
    }
}