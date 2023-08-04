package io.confluent.ps;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;


/**
 * The default SchemaRegistryClient is not caching the calls we need, hence this wrapper class.
 */

public class CachingClient {

    private final SchemaRegistryClient client;


    record SubjectId (String subject, int id) {}
    record SubjectSchema (String subject, ParsedSchema schema) {}

    final Cache<SubjectSchema, Integer> idCache;
    final Cache<SubjectId, ParsedSchema> schemaCache;

    public CachingClient(SchemaRegistryClient client, int cacheSize) {
        this.client = client;

        idCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        schemaCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }


    public ParsedSchema getSchemaBySubjectAndId(String subject, int id) {
        final var subjectId = new SubjectId(subject, id);
        return schemaCache.get(subjectId, key -> {
            try {
                return client.getSchemaBySubjectAndId(key.subject, key.id);
            } catch (IOException | RestClientException e) {
                throw new RuntimeException(e);
            }
        });

    }

    public int getId(String subject, ParsedSchema schema) {
        final var subjectSchema = new SubjectSchema(subject, schema);
        return idCache.get(subjectSchema, key -> {
            try {
                return client.getId(key.subject, key.schema);
            } catch (IOException | RestClientException e) {
               throw new RuntimeException(e);
            }
        });
    }
}
