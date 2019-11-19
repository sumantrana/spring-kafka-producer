package com.sumant.boot.learning.springkafkaproducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaBookProducer {

    @Value("${book.topic}")
    public String bookTopic;

    private KafkaTemplate<String, Book> kafkaTemplate;

    KafkaBookProducer(KafkaTemplate<String, Book> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendBookMessage(Book book){
        ProducerRecord<String, Book> producerRecord = new ProducerRecord<String, Book>(bookTopic, book);
        kafkaTemplate.send(producerRecord);
        kafkaTemplate.flush();
    }
}
