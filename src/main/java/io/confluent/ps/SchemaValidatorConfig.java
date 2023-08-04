package io.confluent.ps;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class SchemaValidatorConfig extends AbstractConfig {

    public static final String INPUT_TOPIC_LIST_CONFIG = "input.topics.list";
    public static final String INPUT_TOPIC_LIST_DOC = "Comma separated list of topic-names to read from.";

    public static final String CACHE_SIZE_CONFIG = "cache.size";
    public static final String CACHE_SIZE_DOC= "Size of the schema-cache.";
    public static final int CACHE_SIZE_DEFAULT = 100;

    public static final String ACCEPTED_TOPIC_SUFFIX_CONFIG = "accepted.topic.suffix";
    public static final String ACCEPTED_TOPIC_SUFFIX_DOC = "Topic-name suffix for accepted messages. Output topics " +
            "for accepted messages are formed via <input-topi> + suffix.";
    public static final String ACCEPTED_TOPIC_SUFFIX_DEFAULT = "_cleaned";


    public static final String REJECTED_TOPIC_SUFFIX_CONFIG = "rejected.topic.suffix";
    public static final String REJECTED_TOPIC_SUFFIX_DOC = "Topic-name suffix for rejected messages. Output topics " +
            "for rejected messages are formed via <input-topi> + suffix.";
    public static final String REJECTED_TOPIC_SUFFIX_DEFAULT = "_rejected";

    static final ConfigDef configDef = new ConfigDef()
            .define(
                    INPUT_TOPIC_LIST_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.Importance.HIGH,
                    INPUT_TOPIC_LIST_DOC
            ).define(
                    CACHE_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    CACHE_SIZE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CACHE_SIZE_DOC
            ).define(
                    ACCEPTED_TOPIC_SUFFIX_CONFIG,
                    ConfigDef.Type.STRING,
                    ACCEPTED_TOPIC_SUFFIX_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    ACCEPTED_TOPIC_SUFFIX_DOC
            ).define(
                    REJECTED_TOPIC_SUFFIX_CONFIG,
                    ConfigDef.Type.STRING,
                    REJECTED_TOPIC_SUFFIX_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    REJECTED_TOPIC_SUFFIX_DOC
            )
            ;

    public SchemaValidatorConfig(Map<?, ?> originals) {
        super(configDef, originals);
    }

    public List<String> inputTopicList() {
        return getList(INPUT_TOPIC_LIST_CONFIG);
    }

    public int cacheSize() {
        return getInt(CACHE_SIZE_CONFIG);
    }

    public String acceptedTopicSuffix() {
        return getString(ACCEPTED_TOPIC_SUFFIX_CONFIG);
    }

    public String rejectedTopicSuffix() {
        return getString(REJECTED_TOPIC_SUFFIX_CONFIG);
    }
}
