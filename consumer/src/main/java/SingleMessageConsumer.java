import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kafka.config.KafkaConfigSingle;
import ru.kafka.messages.Person;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SingleMessageConsumer {
    private final ExecutorService executor;

    public SingleMessageConsumer() {
        executor = Executors.newSingleThreadExecutor();
    }

    public static void main(String[] args) {
        new SingleMessageConsumer().run();
    }

    private void run() {
        Properties properties = KafkaConfigSingle.getKafkaProperties();
        executor.execute(new ConsumerTask(properties));
        // Останавливаем выполнение задач и закрываем ThreadPoolExecutor
        executor.shutdown();
    }

    private static class ConsumerTask implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerTask.class);
        private final Properties properties;

        public ConsumerTask(Properties properties) {
            this.properties = properties;
        }

        @Override
        public void run() {
            try (KafkaConsumer<Long, Person> consumer = new KafkaConsumer<>(properties)) {
                consumer.subscribe(Collections.singletonList(KafkaConfigSingle.TOPIC));

                // Чтение сообщений
                while (true) {
                    ConsumerRecords<Long, Person> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Long, Person> record : records) {
                        logger.info("Received record: key -> {}, value -> {}, partition -> {}, offset -> {}",
                                record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch (Exception e) {
                logger.error("Ошибка обработки: message -> {} ", e.getMessage());
            }

        }
    }

}
