package com.sumant.boot.learning.springkafkaproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class KafkaBookProducer {

    @Value("${book.topic}")
    public String bookTopic;

    private KafkaTemplate<String, Book> kafkaTemplate;

    private BlockingQueue<Book> failedMessageQueue = new ArrayBlockingQueue<Book>(5);

    KafkaBookProducer(KafkaTemplate<String, Book> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendBookMessage(Book book){
        ProducerRecord<String, Book> producerRecord = new ProducerRecord<String, Book>(bookTopic, book);
        producerRecord.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "kReplies".getBytes()));

        ListenableFuture<SendResult<String, Book>> future = kafkaTemplate.send(producerRecord);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Book>>() {

            @Override
            public void onSuccess(SendResult<String, Book> result) {
                System.out.println("Message sent successfully");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Exception while sending message " + ex);
            }

        });

        kafkaTemplate.flush();
    }

}
