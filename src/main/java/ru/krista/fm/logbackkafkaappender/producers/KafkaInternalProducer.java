package ru.krista.fm.logbackkafkaappender.producers;

import ru.krista.fm.logbackkafkaappender.delivery.FailedDeliveryCallback;

import java.util.Map;

public interface KafkaInternalProducer<E> {
    String getKafkaLoggerPrefix();

    void setProducerConfig(Map<String, Object> producerConfig);

    void setFailedDeliveryCallback(FailedDeliveryCallback<E> failedDeliveryCallback);

    void send(String topic, Integer partition, Long timestamp, byte[] key, byte[] payload, E e);

    void close();
}
