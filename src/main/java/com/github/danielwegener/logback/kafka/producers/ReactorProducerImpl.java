package com.github.danielwegener.logback.kafka.producers;

import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.HashMap;
import java.util.Map;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class ReactorProducerImpl<E> implements KafkaInternalProducer<E> {

    /**
     * Kafka clients uses this prefix for its slf4j logging.
     * This appender defers appends of any Kafka logs since it could cause harmful infinite recursion/self feeding effects.
     */
    private static final String KAFKA_LOGGER_PREFIX = KafkaSender.class.getPackage().getName().replaceFirst("\\.sender$", "");

    private KafkaSender<byte[], byte[]> kafkaSender = null;

    private Map<String,Object> producerConfig = new HashMap<String, Object>();
    private FailedDeliveryCallback<E> failedDeliveryCallback;

    public ReactorProducerImpl() { }

    public ReactorProducerImpl(Map<String,Object> producerConfig, FailedDeliveryCallback<E> failedDeliveryCallback ) {
        this.producerConfig = producerConfig;
        this.failedDeliveryCallback = failedDeliveryCallback;
    }

    @Override
    public String getKafkaLoggerPrefix() { return KAFKA_LOGGER_PREFIX; }

    @Override
    public void setProducerConfig(Map<String, Object> producerConfig) {
        this.producerConfig = producerConfig;
    }

    @Override
    public void setFailedDeliveryCallback(FailedDeliveryCallback<E> failedDeliveryCallback) {
        this.failedDeliveryCallback = failedDeliveryCallback;
    }

    @Override
    public void send(String topic, Integer partition, Long timestamp, byte[] key, byte[] payload, E e) {
        final SenderRecord<byte[], byte[], byte[]> record = SenderRecord.create(topic, partition, timestamp, key, payload, key);

        if (kafkaSender != null) {
            kafkaSender.send(Mono.just(record)).doOnError(x -> failedDeliveryCallback.onFailedDelivery(e, x)).subscribe();
        }

    }

    public void init() {
        kafkaSender = createKafkaSender();
    }

    @Override
    public void close() {
        if (kafkaSender != null) {
            kafkaSender.close();
            kafkaSender = null;
        }
    }

    protected SenderOptions<byte[], byte[]> createSenderOptions() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfig.get(BOOTSTRAP_SERVERS_CONFIG));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        SenderOptions<byte[], byte[]> senderOptions = SenderOptions.<byte[], byte[]>create(producerProps); //.maxInFlight(1024);

        return senderOptions;
    }

    protected KafkaSender<byte[], byte[]> createKafkaSender() {
        KafkaSender<byte[], byte[]> sender = KafkaSender.create(createSenderOptions());
        return sender;
    }

}
