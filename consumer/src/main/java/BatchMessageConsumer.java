import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kafka.config.KafkaConfigBatch;
import ru.kafka.messages.Person;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchMessageConsumer {

    private final ExecutorService executor;

    public BatchMessageConsumer() {
        executor = Executors.newSingleThreadExecutor();
    }

    public static void main(String[] args) {
        (new BatchMessageConsumer()).run();
    }

    private void run() {
        Properties properties = KafkaConfigBatch.getKafkaProperties();
        executor.execute(new BatchMessageConsumer.ConsumerTask(properties));
        // Останавливаем выполнение задач и закрываем ThreadPoolExecutor
        executor.shutdown();
    }

    private static class ConsumerTask implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(BatchMessageConsumer.ConsumerTask.class);
        private final Properties properties;

        public ConsumerTask(Properties properties) {
            this.properties = properties;
        }

        @Override
        public void run() {
            try (KafkaConsumer<Long, Person> consumer = new KafkaConsumer<>(properties)) {
                consumer.subscribe(Collections.singletonList(KafkaConfigBatch.TOPIC));

                // Чтение сообщений
                while (true) {
                    ConsumerRecords<Long, Person> records = consumer.poll(Duration.ofMillis(700));
                    if (records.count() == 0){
                        continue;
                    }

                    logger.info("Начало чтение пакета записей");
                    for (ConsumerRecord<Long, Person> record : records) {
                        logger.info("Received record: key -> {}, value -> {}, partition -> {}, offset -> {}",
                                record.key(), record.value(), record.partition(), record.offset());
                    }

                    consumer.commitSync();
                    logger.info("Чтение пакета записей -> {}", records.count());
                }
            } catch (Exception e) {
                logger.error("Ошибка обработки: exception message -> {} ", e.getMessage());
            }

        }
    }


}
