package io.confluent.ps;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

class SchemaIdValidationTransformer implements ValueTransformer<byte[], SchemaIdValidator.ValueResult> {

    private static final Logger log = LoggerFactory.getLogger(SchemaIdValidationTransformer.class);

    static final int magicBytePosition = 0; // as specified by the schema registry wire format
    static final int schemaIdPosition = 1;

    public static final String REASON_HEADER_KEY = "reason";

    private ProcessorContext context;

    private final CachingClient client;
    private final SubjectNameStrategy strategy = new TopicNameStrategy();

    private final Function<String, String> cleanTopicForTopic;
    private final Function<String, String> dlqTopicForTopic;

    SchemaIdValidationTransformer(SchemaRegistryClient client, SchemaValidatorConfig config) {
        this.client = new CachingClient(client, config.cacheSize());
        this.cleanTopicForTopic = topic -> topic + config.acceptedTopicSuffix();
        this.dlqTopicForTopic = topic -> topic + config.rejectedTopicSuffix();
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    SchemaIdValidator.ValueResult pass(byte[] value) {
        return new SchemaIdValidator.ValueResult(value, cleanTopicForTopic.apply(context.topic()));
    }

    SchemaIdValidator.ValueResult fail(byte[] value, String reason) {
        addReasonHeaderToContext(reason);
        return new SchemaIdValidator.ValueResult(value, dlqTopicForTopic.apply(context.topic()));
    }

    @Override
    public SchemaIdValidator.ValueResult transform(byte[] value) {
        final var topic = context.topic();
        if (value == null) {
            return pass(value);
        }

        if (value.length < 5) {
            String msg = String.format("Value size %d does not conform to wire format, should be at least 5 bytes", value.length);
            return fail(value, msg);
        }

        final ByteBuffer buffer = ByteBuffer.wrap(value);

        if (buffer.get(magicBytePosition) != 0) {
            // so far, we only have one format version (magic-byte == 0);
            final String reason = String.format("Unknown magic byte '%d' in topic '%s'.", buffer.get(magicBytePosition), topic);
            log.info(reason);
            return fail(value, reason);
        }
        buffer.order(ByteOrder.BIG_ENDIAN); //ensure standard network byte order
        final int id = buffer.getInt(schemaIdPosition);

        try {
            log.info("Getting schema for id {}", id);

            String subject = null;
            if (!strategy.usesSchema()) {
                subject = strategy.subjectName(topic, false, null);
            }
            final var schema = client.getSchemaBySubjectAndId(subject, id);

            if (subject == null) {
                subject = strategy.subjectName(topic, false, schema);
            }
            if (id == client.getId(subject, schema)) {
                return pass(value);
            } else {
                String msg = String.format("ID %d not matching saved id %d", id, client.getId(subject, schema));
                return fail(value, msg);
            }

        } catch (RuntimeException e) {
            final var cause = e.getCause();
            if (cause instanceof IOException ioException) {
                final var msg = "IOException: " + ioException.getCause() + " " + ioException.getMessage();
                return fail(value, msg);
            }
            if (cause instanceof RestClientException clientException) {
                    log.info("RestClientException, cause: {}", clientException.getMessage());
                final var errorCode = clientException.getErrorCode();
//                if (errorCode == 40301) {
//                    //unauthorized, so let's fail
//                    final var msg = "Not authorized to access schema registry";
//                    log.error(msg);
//                    throw new RuntimeException(msg);
//                }
                return fail(value, "Subject not found");
            }
            return fail(value, e.getMessage());
        }
    }

    void addReasonHeaderToContext(String reason) {
        context.headers().add(REASON_HEADER_KEY, reason.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        //nothing to do
    }
}
