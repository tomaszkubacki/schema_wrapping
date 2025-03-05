package net.tk.kafka.connect.converter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;


public class SimpleSchemaWrappingConverterConfig extends ConverterConfig {

    public static final String ENCODING_CONFIG = "converter.encoding";
    public static final String ENCODING_DEFAULT = StandardCharsets.UTF_8.name();
    private static final String ENCODING_DOC = "The name of the Java character set to use for encoding strings as byte arrays.";
    private static final String ENCODING_DISPLAY = "Encoding";

    public static final String CONTENT_NAME_CONFIG = "converter.content.name";
    public static final String CONTENT_NAME_DEFAULT = "content";
    private static final String CONTENT_NAME_DOC = "The name of content name filed defaults to 'content'.";
    private static final String CONTENT_NAME_DISPLAY = "ContentFieldName";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(ENCODING_CONFIG, ConfigDef.Type.STRING, ENCODING_DEFAULT, ConfigDef.Importance.HIGH, ENCODING_DOC, null, -1, ConfigDef.Width.MEDIUM,
                ENCODING_DISPLAY);
        CONFIG.define(CONTENT_NAME_CONFIG, ConfigDef.Type.STRING, CONTENT_NAME_DEFAULT, ConfigDef.Importance.LOW, CONTENT_NAME_DOC, null, -1, ConfigDef.Width.MEDIUM,
                CONTENT_NAME_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public SimpleSchemaWrappingConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Get the string encoding.
     *
     * @return the encoding; never null
     */
    public String encoding() {
        return getString(ENCODING_CONFIG);
    }

    /**
     * Get the content field name.
     *
     * @return the content field name; never null
     */
    public String contentName() {
        return getString(CONTENT_NAME_CONFIG);
    }
}