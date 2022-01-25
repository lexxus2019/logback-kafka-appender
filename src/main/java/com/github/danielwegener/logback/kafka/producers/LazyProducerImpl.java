package com.github.danielwegener.logback.kafka.producers;

import com.github.danielwegener.logback.kafka.delivery.AsynchronousDeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

import static ch.qos.logback.classic.util.StatusViaSLF4JLoggerFactory.addError;

public class LazyProducerImpl<E> implements KafkaInternalProducer<E> {

    private volatile Producer<byte[], byte[]> lazyProducer;
    private DeliveryStrategy deliveryStrategy;
    private Map<String,Object> producerConfig = new HashMap<String, Object>();
    private FailedDeliveryCallback<E> failedDeliveryCallback;

    /**
     * Kafka clients uses this prefix for its slf4j logging.
     * This appender defers appends of any Kafka logs since it could cause harmful infinite recursion/self feeding effects.
     */
    private static final String KAFKA_LOGGER_PREFIX = KafkaProducer.class.getPackage().getName().replaceFirst("\\.producer$", "");

    public LazyProducerImpl() { }

    public LazyProducerImpl(Map<String,Object> producerConfig, FailedDeliveryCallback<E> failedDeliveryCallback) {
        this.producerConfig = producerConfig;
        this.failedDeliveryCallback = failedDeliveryCallback;
        deliveryStrategy = new AsynchronousDeliveryStrategy();
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
        Producer<byte[], byte[]> producer = get();
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, partition, timestamp, key, payload);

        if (producer != null) {
            deliveryStrategy.send(producer, record, e, failedDeliveryCallback);
        } else {
            failedDeliveryCallback.onFailedDelivery(e, null);
        }
    }

    @Override
    public void close() {
        if (lazyProducer != null) {
            lazyProducer.close();
            lazyProducer = null;
        }
    }

    protected Producer<byte[], byte[]> createProducer() {
        return new org.apache.kafka.clients.producer.KafkaProducer<>(new HashMap<>(producerConfig));
    }

    public Producer<byte[], byte[]> get() {
        Producer<byte[], byte[]> result = this.lazyProducer;
        if (result == null) {
            synchronized(this) {
                result = this.lazyProducer;
                if(result == null) {
                    this.lazyProducer = result = this.initialize();
                }
            }
        }

        return result;
    }

    protected Producer<byte[], byte[]> initialize() {
        Producer<byte[], byte[]> producer = null;
        try {
            producer = createProducer();
        } catch (Exception e) {
            addError("error creating producer", e);
        }
        return producer;
    }
}
