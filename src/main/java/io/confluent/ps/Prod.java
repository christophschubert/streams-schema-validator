package io.confluent.ps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Prod {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        final var props = new Properties();
        props.load(new FileInputStream("ccloud.properties"));

        final var producer = new KafkaProducer<>(props,new ByteArraySerializer(), new ByteArraySerializer());

        byte id = 1;

        final byte[] value = {0, 0, 1, (byte)134, (byte)161};

        producer.send(new ProducerRecord<>("input", value)).get();
        producer.flush();
        System.out.println("send message with ID " + id);

    }
}
