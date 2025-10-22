package ru.alex3koval.eventingImpl.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.vo.EventStatus;

import java.io.IOException;
import java.time.LocalDateTime;

@RequiredArgsConstructor
public class EventDeserializer implements Deserializer<Event> {
    private final ObjectMapper objectMapper;

    @Override
    public Event deserialize(String topic, byte[] data) {
        try {
            JsonNode jsonNode = objectMapper.readTree(data);
            short rawEventStatus = jsonNode.get("status").shortValue();

            return new Event(
                jsonNode.get("id").asText(),
                jsonNode.get("name").asText(),
                jsonNode.get("json").asText(),
                EventStatus
                    .of(rawEventStatus)
                    .orElseThrow(() -> new RuntimeException(String.format("Не удалось десериализовать из: %s", rawEventStatus))),
                LocalDateTime.parse(jsonNode.get("createdAt").asText())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
