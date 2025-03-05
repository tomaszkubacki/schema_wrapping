package net.tk.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class AddMetadataConfig extends AbstractConfig {

    public AddMetadataConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, false);
    }

}