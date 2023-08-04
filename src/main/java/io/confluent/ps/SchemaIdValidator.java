package io.confluent.ps;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SchemaIdValidator {

    private static final Logger log = LoggerFactory.getLogger(SchemaIdValidator.class);

    record ValueResult(byte[] value, String topic) {}


    static class Extractor<K> implements TopicNameExtractor<K, ValueResult> {
        @Override
        public String extract(K key, ValueResult value, RecordContext recordContext) {
            return value.topic();
        }
    }

    // we need to provide a ValueResult to the last operation ('.to') in the topology, hence we implement this
    // serializer/deserializer-pair which is just stripping the topic-name and 'serializing' the value;
    static class Bas implements Serializer<ValueResult> {
        @Override
        public byte[] serialize(String topic, ValueResult data) {
            return data.value();
        }

    }

    static class DBas implements Deserializer<ValueResult> {
        @Override
        public ValueResult deserialize(String topic, byte[] data) {
            return new ValueResult(data, topic);
        }
    }


    static Properties loadPropertiesOrDefault(String... args) {
        var properties = new Properties();
        if (args.length > 0) {
            final var propertyPath = args[0];
            log.info("Loading properties from {}", propertyPath);
            try {
                properties.load(new FileInputStream(propertyPath));
            } catch (IOException e) {
                log.error("Error loading properties from path {}:", propertyPath, e);
                //fail fast
                System.exit(1);
            }
        } else {
            //set defaults for local testing
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "schema-id-validator");
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(SchemaValidatorConfig.INPUT_TOPIC_LIST_CONFIG, "input");
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        }
        //use ByteArraySerde because we will perform deserialization/validity checks 'in stream'
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        return properties;
    }

    public static void main(String... args) throws RestClientException, IOException {

        final var properties = loadPropertiesOrDefault(args);
        final SchemaValidatorConfig config = new SchemaValidatorConfig(properties);

        System.out.println(config.originals());

        final SchemaRegistryClient client = new CachedSchemaRegistryClient(
                properties.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),
                10, // caching is implemented in TimedCachingClient
                config.originals()
        );


        final var builder = new StreamsBuilder();
        final KStream<byte[], byte[]> input = builder.stream(config.inputTopicList());
        input.transformValues(() -> new SchemaIdValidationTransformer(client, config))
            .to(new Extractor<>(), Produced.valueSerde(Serdes.serdeFrom(new Bas(), new DBas())));

        final var topology = builder.build();

        final var app = new KafkaStreams(topology, properties);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
        log.info("app started");
    }

}
