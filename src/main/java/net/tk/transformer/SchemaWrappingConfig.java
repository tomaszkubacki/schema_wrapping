package net.tk.transformer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SchemaWrappingConfig extends AbstractConfig {

    public SchemaWrappingConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, false);
    }

}