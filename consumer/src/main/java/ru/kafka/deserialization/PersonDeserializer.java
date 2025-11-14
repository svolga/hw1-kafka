package ru.kafka.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import ru.kafka.messages.Person;

import java.io.IOException;

public class PersonDeserializer implements Deserializer<Person> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Person deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return objectMapper.readValue(data, Person.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
