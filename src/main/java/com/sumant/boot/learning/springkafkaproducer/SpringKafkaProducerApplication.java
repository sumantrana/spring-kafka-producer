package com.sumant.boot.learning.springkafkaproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaProducerApplication implements CommandLineRunner {

    @Autowired
    KafkaBookProducer kafkaBookProducer;


    public static void main(String[] args) {

        SpringApplication.run(SpringKafkaProducerApplication.class, args);

    }

    @Override
    public void run(String... args) throws Exception {
        Book book = Book.builder().name("Sumant").value(10).build();
        kafkaBookProducer.sendBookMessage(book);
        kafkaBookProducer.sendBookMessage(book);
    }
}
