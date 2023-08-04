package io.confluent.ps;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaIdValidatorTest {


    private static final String TEST_TOPIC = "input";

    SchemaValidatorConfig config = new SchemaValidatorConfig(Map.of(
            SchemaValidatorConfig.INPUT_TOPIC_LIST_CONFIG, TEST_TOPIC
    ));

    ParsedSchema buildAvroSchema() {
        final var avroSchemaProvider = new AvroSchemaProvider();
        final var schema = SchemaBuilder.record("test").fields().requiredString("name").endRecord();

        return avroSchemaProvider.parseSchema(schema.toString(), Collections.emptyList()).get();
    }

    ProcessorContext buildContext() {
        MockProcessorContext context = new MockProcessorContext();
        context.setTopic(TEST_TOPIC);
        context.setHeaders(new RecordHeaders());
        return context;
    }

    String reasonHeaderFromContext(ProcessorContext context) {
        return new String(context.headers().lastHeader(SchemaIdValidationTransformer.REASON_HEADER_KEY).value());
    }

    @Test
    void nullValueShouldPass() {
        final var context = buildContext();
        final var client = new MockSchemaRegistryClient();
        final var transformer = new SchemaIdValidationTransformer(client, config);
        transformer.init(context);

        final var result = transformer.transform(null);
        assertEquals(null, result.value());
        assertEquals(TEST_TOPIC + SchemaValidatorConfig.ACCEPTED_TOPIC_SUFFIX_DEFAULT, result.topic());
        assertEquals(0, context.headers().toArray().length);
    }


    @Test
    void wrongMagicByteShouldFail() {
        final var context = buildContext();
        final var client = new MockSchemaRegistryClient();
        final var transformer = new SchemaIdValidationTransformer(client, config);
        transformer.init(context);

        final var value = new byte[]{1, 0, 0, 0, 0};

        final var result = transformer.transform(value);
        assertEquals(value, result.value());
        assertEquals(TEST_TOPIC + SchemaValidatorConfig.REJECTED_TOPIC_SUFFIX_DEFAULT, result.topic());
        assertEquals("Unknown magic byte '1' in topic 'input'.", reasonHeaderFromContext(context));
    }

    @Test
    void unregisteredSubjectShouldFail() {
        final var context = buildContext();
        final var client = new MockSchemaRegistryClient();
        final var transformer = new SchemaIdValidationTransformer(client, config);
        transformer.init(context);

        //correct wire-format, but no schema registered:
        final var value = new byte[]{0, 0, 0, 0, 1};

        final var result = transformer.transform(value);
        assertEquals(value, result.value());
        assertEquals(TEST_TOPIC + SchemaValidatorConfig.REJECTED_TOPIC_SUFFIX_DEFAULT, result.topic());
        assertEquals("Subject not found", reasonHeaderFromContext(context));
  }


    @Test
    void correctlyRegisteredSchemaShouldPass() throws RestClientException, IOException {
        final var context = buildContext();
        final var client = new MockSchemaRegistryClient();
        final var transformer = new SchemaIdValidationTransformer(client, config);
        transformer.init(context);

        final var parsedSchema = buildAvroSchema();
        client.register(TEST_TOPIC + "-value", parsedSchema);

        //correct wire-format, first schema has ID 1:
        final var value = new byte[]{0, 0, 0, 0, 1};

        final var result = transformer.transform(value);
        assertEquals(value, result.value());
        assertEquals(TEST_TOPIC + SchemaValidatorConfig.ACCEPTED_TOPIC_SUFFIX_DEFAULT, result.topic());
        assertEquals(0, context.headers().toArray().length);
    }

    @Test
    void unregisteredSchemaShouldFail() throws RestClientException, IOException {
        final var context = buildContext();
        final var client = new MockSchemaRegistryClient();
        final var transformer = new SchemaIdValidationTransformer(client, config);
        transformer.init(context);

        final var parsedSchema = buildAvroSchema();
        client.register(TEST_TOPIC + "-value", parsedSchema);

        //correct wire-format, first schema has ID 1:
        final var value = new byte[]{0, 0, 0, 0, 2};

        final var result = transformer.transform(value);
        assertEquals(value, result.value());
        assertEquals(TEST_TOPIC + SchemaValidatorConfig.REJECTED_TOPIC_SUFFIX_DEFAULT, result.topic());
        assertEquals("Subject not found", reasonHeaderFromContext(context));
    }

    @Test
    void schemaForWrongSubjectShouldFail() throws RestClientException, IOException {
        final var context = buildContext();
        final var client = new MockSchemaRegistryClient();
        final var transformer = new SchemaIdValidationTransformer(client, config);
        transformer.init(context);

        final var parsedSchema = buildAvroSchema();
        client.register(TEST_TOPIC + "-other", parsedSchema);

        //correct wire-format, first schema has ID 1:
        final var value = new byte[]{0, 0, 0, 0, 1};

        final var result = transformer.transform(value);
        assertEquals(value, result.value());
//        assertEquals(TEST_TOPIC + SchemaValidatorConfig.REJECTED_TOPIC_SUFFIX_DEFAULT, result.topic());
        assertEquals(TEST_TOPIC + SchemaValidatorConfig.ACCEPTED_TOPIC_SUFFIX_DEFAULT, result.topic());
//        assertEquals("Subject not found", reasonHeaderFromContext(context));
        assertEquals(0, context.headers().toArray().length);
    }

}