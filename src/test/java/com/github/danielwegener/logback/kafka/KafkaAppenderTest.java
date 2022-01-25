package com.github.danielwegener.logback.kafka;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.BasicStatusManager;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.ErrorStatus;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;
import com.github.danielwegener.logback.kafka.producers.KafkaInternalProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Map;

public class KafkaAppenderTest {

    private final KafkaAppender<ILoggingEvent> unit = new KafkaAppender<>();
    private final LoggerContext ctx = new LoggerContext();
    @SuppressWarnings("unchecked")
    private final Encoder<ILoggingEvent> encoder =  mock(Encoder.class);
    private final KeyingStrategy<ILoggingEvent> keyingStrategy =  mock(KeyingStrategy.class);
    @SuppressWarnings("unchecked")
    private final KafkaInternalProducer kafkaInternalProducer =  mock(KafkaInternalProducer.class);

    @Before
    public void before() {
        ctx.setName("testctx");
        ctx.setStatusManager(new BasicStatusManager());
        unit.setContext(ctx);
        unit.setName("kafkaAppenderBase");
        unit.setEncoder(encoder);
        unit.setTopic("topic");
        unit.addProducerConfig("bootstrap.servers=localhost:1234");
        unit.setKeyingStrategy(keyingStrategy);
        unit.setInternalProducer(kafkaInternalProducer);
        ctx.start();
    }

    @After
    public void after() {
        ctx.stop();
        unit.stop();
    }

    @Test
    public void testPerfectStartAndStop() {
        unit.start();
        assertTrue("isStarted", unit.isStarted());
        unit.stop();
        assertFalse("isStopped", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(), empty());
        verifyZeroInteractions(encoder, keyingStrategy, kafkaInternalProducer);
    }

    @Test
    public void testDontStartWithoutTopic() {
        unit.setTopic(null);
        unit.start();
        assertFalse("isStarted", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(),
                hasItem(new ErrorStatus("No topic set for the appender named [\"kafkaAppenderBase\"].", null)));
    }

    @Test
    public void testDontStartWithoutBootstrapServers() {
        unit.getProducerConfig().clear();
        unit.start();
        assertFalse("isStarted", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(),
                hasItem(new ErrorStatus("No \"bootstrap.servers\" set for the appender named [\"kafkaAppenderBase\"].", null)));
    }

    @Test
    public void testDontStartWithoutEncoder() {
        unit.setEncoder(null);
        unit.start();
        assertFalse("isStarted", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(),
                hasItem(new ErrorStatus("No encoder set for the appender named [\"kafkaAppenderBase\"].", null)));
    }

    @Test
    public void testAppendUsesKeying() {
        when(encoder.encode(any(ILoggingEvent.class))).thenReturn(new byte[]{0x00, 0x00});
        unit.start();
        final LoggingEvent evt = new LoggingEvent("fqcn",ctx.getLogger("logger"), Level.ALL, "message", null, new Object[0]);
        unit.append(evt);
        unit.internalProducer.setFailedDeliveryCallback(any(FailedDeliveryCallback.class));
        unit.internalProducer.setProducerConfig(any(Map.class));
        verify(kafkaInternalProducer).send(any(String.class), any(Integer.class), any(Long.class), any(byte[].class), any(byte[].class), eq(evt));
        verify(keyingStrategy).createKey(same(evt));
        verify(kafkaInternalProducer).send(any(String.class), any(Integer.class), any(Long.class), any(byte[].class), any(byte[].class), eq(evt));
    }

    @Test
    public void testDeferredAppend() {
        when(encoder.encode(any(ILoggingEvent.class))).thenReturn(new byte[]{0x00, 0x00});
        unit.start();
        final LoggingEvent deferredEvent = new LoggingEvent("fqcn",ctx.getLogger("org.apache.kafka.clients.logger"), Level.ALL, "deferred message", null, new Object[0]);
        unit.doAppend(deferredEvent);

        verify(kafkaInternalProducer).send(any(String.class), any(Integer.class), any(Long.class), any(byte[].class), any(byte[].class), eq(deferredEvent));

        final LoggingEvent evt = new LoggingEvent("fqcn",ctx.getLogger("logger"), Level.ALL, "message", null, new Object[0]);
        unit.doAppend(evt);
        verify(kafkaInternalProducer).send(any(String.class), any(Integer.class), any(Long.class), any(byte[].class), any(byte[].class), eq(deferredEvent));
        verify(kafkaInternalProducer).send(any(String.class), any(Integer.class), any(Long.class), any(byte[].class), any(byte[].class), eq(evt));
    }

    @Test
    public void testKafkaLoggerPrefix() throws ReflectiveOperationException {
        Field constField = KafkaAppender.class.getDeclaredField("KAFKA_LOGGER_PREFIX");
        if (!constField.isAccessible()) {
            constField.setAccessible(true);
        }
        String constValue = (String) constField.get(null);
        assertThat(constValue, equalTo("org.apache.kafka.clients"));
    }


}
