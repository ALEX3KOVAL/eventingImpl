package ru.alex3koval.eventingImpl.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;
import ru.alex3koval.eventingContract.Event;

@RequiredArgsConstructor
public class EventSerializer implements Serializer<Event> {
    private final ObjectMapper objectMapper;

    @Override
    public byte[] serialize(String topic, Event data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
