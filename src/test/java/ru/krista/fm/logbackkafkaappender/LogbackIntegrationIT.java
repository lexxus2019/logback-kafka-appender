package ru.krista.fm.logbackkafkaappender;

import ru.krista.fm.logbackkafkaappender.util.TestKafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LogbackIntegrationIT {

    @Rule
    public ErrorCollector collector= new ErrorCollector();

    private TestKafka kafka;
    private org.slf4j.Logger logger;

    @Before
    public void beforeLogSystemInit() throws IOException, InterruptedException {
        kafka = TestKafka.createTestKafka(Collections.singletonList(9093));
        logger = LoggerFactory.getLogger("LogbackIntegrationIT");

    }

    @After
    public void tearDown() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }


    @Test
    public void testLogging() {

        for (int i = 0; i<1000; ++i) {
            logger.info("message"+(i));
        }

        final KafkaConsumer<byte[], byte[]> client = kafka.createClient();
        client.assign(Collections.singletonList(new TopicPartition("logs", 0)));
        client.seekToBeginning(Collections.singletonList(new TopicPartition("logs", 0)));


        int no = 0;

        ConsumerRecords<byte[],byte[]> poll = client.poll(1000);
        while(!poll.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> consumerRecord : poll) {
                final String messageFromKafka = new String(consumerRecord.value(), UTF8);
                assertThat(messageFromKafka, Matchers.equalTo("message"+no));
                ++no;
            }
            poll = client.poll(1000);
        }

        assertEquals(1000, no);

    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

}
