
package ru.kafka;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.kafka.service.ProducerService;

@SpringBootApplication
public class ProducerApp {

    public static void main(String[] args) {
        ProducerService producerService = new ProducerService();
        producerService.sendMessage();
    }

}
