package ru.krista.fm.logbackkafkaappender.util;

import org.junit.Test;
import reactor.kafka.sender.KafkaSender;

public class ClassTest {

    @Test
    public void classTest() {
        System.out.println(KafkaSender.class.getPackage().getName());
        System.out.println(KafkaSender.class.getPackage().getName().replaceFirst("\\.sender$", ""));
    }
}
