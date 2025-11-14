package ru.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import ru.kafka.deserialization.PersonDeserializer;

import java.util.Properties;

public class KafkaConfigSingle {


    public static final String TOPIC = "my-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";
    private static final String GROUP_ID = "consumer-group-single";

    public static Properties getKafkaProperties() {
        Properties properties = new Properties();

        /** Подключения к Kafka-брокеру BOOTSTRAP_SERVERS */
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        /** Идентификатор группы потребителей (consumer group ID) */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        /** Использование LongDeserializer для десериализации ключей (Key) */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        /** Использование PersonDeserializer для десериализации значений (Value) */
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonDeserializer.class);

        /** Управление поведением потребителя при первом подключении к топику или при потере сохраненного смещения:
         * - "earliest": начинает считывать сообщения с самого начала топика
         * - "latest": начинает считывать сообщения с самого последнего доступного смещения.
         * - "none": если нет сохраненного смещения, потребитель выбрасывает исключение.
         * - "error": потребитель выбрасывает исключение при отсутствии сохраненного смещения или если смещение находится
         * за пределами диапазона доступных смещений.
         * */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /** Включение автоматической фиксации смещений (enable.auto.commit = true).
         * По умолчанию параметр enable.auto.commit установлен в true - потребитель автоматически фиксирует оффсет после
         * обработки каждого пакета сообщений
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        return properties;
    }


}
