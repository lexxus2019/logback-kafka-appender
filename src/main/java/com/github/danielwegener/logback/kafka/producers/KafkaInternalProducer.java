package com.github.danielwegener.logback.kafka.producers;

import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;

import java.util.Map;

public interface KafkaInternalProducer<E> {
    String getKafkaLoggerPrefix();

    void setProducerConfig(Map<String, Object> producerConfig);

    void setFailedDeliveryCallback(FailedDeliveryCallback<E> failedDeliveryCallback);

    void send(String topic, Integer partition, Long timestamp, byte[] key, byte[] payload, E e);

    void close();
}
