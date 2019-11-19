package com.sumant.boot.learning.springkafkaproducer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@SpringBootTest
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"${book.topic}"})
public class KafkaBookProducerTestConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBookProducerTestConsumer.class);

    @Value("${book.topic}")
    private String senderTopic;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaBookProducer producer;

    private Consumer<String, Book> consumer;



    @BeforeEach
    public void setUp() throws Exception {

        //embeddedKafkaBroker.addTopics(senderTopic);

        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("sender", "false", embeddedKafkaBroker);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, Book> consumerFactory =
                new DefaultKafkaConsumerFactory<String, Book>(
                        consumerProperties, new StringDeserializer(), new JsonDeserializer<>(Book.class));

        consumer = consumerFactory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, senderTopic);

    }

    @Test
    public void testSend() throws InterruptedException {

        // send the message
        Book messageBook = Book.builder().name("TestBook").value(10).build();
        producer.sendBookMessage(messageBook);

        // check that the message was received
        ConsumerRecord<String, Book> received = KafkaTestUtils.getSingleRecord(consumer, senderTopic);

        // Hamcrest Matchers to check the value
        assertThat(received, hasValue(messageBook));

        // AssertJ Condition to check the key
        assertThat(received).has(key(null));
    }

}
