package ru.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.kafka.config.KafkaConfig;
import ru.kafka.messages.Person;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class ProducerService {

    private final Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private static final int MAX_MESSAGE_COUNT = 100;

    public void sendMessage() {

        logger.info("Start sending message");

        try (KafkaProducer<Long, Person> producer = new KafkaProducer<>(KafkaConfig.getProperties())) {

            for (int i = 0; i < MAX_MESSAGE_COUNT; i++) {

                Person person = createPerson(i);

                /**
                 * Конструктор ProducerRecord(topic, partition, timestamp, key, value) принимает в качестве аргументов:
                 * - topic - номер топика
                 * - partition - номер партиции           (опция)
                 * - timestamp - время создания сообщения (опция)
                 * - key - ключ id экземпляра Person      (опция)
                 * - value - объект Person
                 *
                 * Варианты конструкторов:
                 * - ProducerRecord(topic, value)
                 * - ProducerRecord(topic, key, value)
                 * - ProducerRecord(topic, partition, key, value)
                 * - ProducerRecord(topic, partition, key, value, headers)
                 */

                long timestamp = System.currentTimeMillis();
                ProducerRecord<Long, Person> producerRecord = new ProducerRecord<>(
                        KafkaConfig.TOPIC,
                        KafkaConfig.PARTITION,
                        timestamp,
                        person.id(),
                        person
                );

                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Ошибка при отправке сообщения: {}", exception.getMessage(), exception);
                    } else {
                        logger.info("Сообщение отправлено успешно: key -> {}, value -> {}, partition -> {}," +
                                        "offset -> {}",
                                person.id(), person, metadata.partition(), metadata.offset());
                    }
                });
                logger.info("Отправлено сообщение: i -> {}, value -> {}", i, person);
                Thread.sleep(1000);
            }
            logger.info("Отправка завершена");
        } catch (Exception e) {
            logger.error("Ошибка при отправке сообщений в Kafka", e);
        }
    }

    private static Person createPerson(int index) {
        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss"));
        return new Person(index, "FirstName-" + currentTime, "LastName" + index, 1 + index);
    }


}
