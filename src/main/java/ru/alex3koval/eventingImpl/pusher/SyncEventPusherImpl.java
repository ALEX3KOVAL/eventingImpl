package ru.alex3koval.eventingImpl.pusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.SyncEventPusher;
import ru.alex3koval.eventingContract.vo.EventStatus;

import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor
public class SyncEventPusherImpl implements SyncEventPusher {
    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void push(String topic, EventStatus status, Object payload) throws InterruptedException {
        if (payload instanceof String) {
            throw new RuntimeException("Payload имеет тип String, но не передано имя события");
        }

        pushEvent(
            topic,
            status,
            payload,
            payload.getClass().getCanonicalName()
        );
    }

    @Override
    public void push(String topic, EventStatus status, Object payload, String eventName) {
        pushEvent(
            topic,
            status,
            payload,
            eventName
        );
    }

    private void pushEvent(String topic, EventStatus status, Object payload, String eventName) {
        try {
            String eventJson = payload instanceof String ? (String)payload : objectMapper.writeValueAsString(payload);

            var future = kafkaTemplate.send(
                topic,
                new Event(
                    eventName,
                    eventJson,
                    status
                )
            );

            future.get();
        } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
