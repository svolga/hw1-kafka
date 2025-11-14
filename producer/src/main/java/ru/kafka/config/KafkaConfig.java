package ru.kafka.config;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import ru.kafka.serialization.PersonSerializer;

import java.util.Properties;

public class KafkaConfig {

    public static final String TOPIC = "my-topic";
    public static final int PARTITION = 0;
    private static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";

    public static Properties getProperties() {
        Properties properties = new Properties();

        /** Подключения к Kafka-брокеру BOOTSTRAP_SERVERS */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        /** Использование StringSerializer для сериализации ключей .
         *  StringSerializer.class в контексте Apache Kafka представляет собой реализацию интерфейса Serializer
         *  из клиентской библиотеки Kafka, которая используется для сериализации объектов типа String в байтовый формат.
         */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        /** Использование PersonSerializer для сериализации значения (Value) */
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer.class.getName());

        /** Включение идемпотентности для продюсера, гарантия exactly once для продюсера */
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        /** Сколько узлов должны подтвердить получение записи, прежде чем считать ее успешно записанной:
         *  - acks=0: продюсер не будет ждать подтверждений от брокера
         *  - acks=1: продюсер будет ждать подтверждения от лидера партиции, но не от всех реплик
         *  - acks=all продюсер будет ждать подтверждений от всех реплик (самая надежная настройка)
         */
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        /** Метод сжатия данных и используемые алгоритмы:
         *  - "none" - без сжатия,
         *  - "gzip" - алгоритм Gzip,
         *  - "snappy" - алгоритм Snappy (Google),
         *  - "lz4" - алгоритм LZ4;
         */
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        /** delivery.timeout.ms - максимальное время ожидания для успешной отправки сообщения.
         * Это включает время, которое сообщение находится в очереди, а также все попытки повторной отправки.
         * По умолчанию  установлено в 120000 миллисекунд (2 минуты).
         */
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);

        /** retry.backoff.ms - время задержки между попытками повторной отправки сообщений в случае возникновения ошибок при отправке.
         * Значение по умолчанию установлено в 100 миллисекунд.
         */
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 200);

        return properties;
    }



}
