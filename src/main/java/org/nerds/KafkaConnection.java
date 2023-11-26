package org.nerds;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Getter
public class KafkaConnection {

    private static KafkaConnection instance;
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaConsumer<String, byte[]> consumer;

    private KafkaConnection() {
        // Initialize KafkaProducer
        Properties producerProps = new Properties();
        String bootstrapServers = "192.168.29.226:9092";
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);

        // Initialize KafkaConsumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "trade-lovers");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(consumerProps);

    }

    public static KafkaConnection getInstance() {
        synchronized (KafkaConnection.class) {
            if (instance == null) instance = new KafkaConnection();
        }
        return instance;
    }


    /*public void verify() {
        Record<String,byte[]> record
    }*/

}
