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
    public void push(
        String id,
        String topic,
        EventStatus status,
        Object payload
    ) {
        if (payload instanceof String) {
            throw new RuntimeException("Payload имеет тип String, но не передано имя события");
        }

        pushEvent(
            id,
            topic,
            status,
            payload,
            payload.getClass().getCanonicalName()
        );
    }

    @Override
    public void push(
        String id,
        String topic,
        EventStatus status,
        Object payload,
        String eventName
    ) {
        pushEvent(
            id,
            topic,
            status,
            payload,
            eventName
        );
    }

    @Override
    public void push(String topic, EventStatus status, Object payload) {
        if (payload instanceof String) {
            throw new RuntimeException("Payload имеет тип String, но не передано имя события");
        }

        pushEvent(
            null,
            topic,
            status,
            payload,
            payload.getClass().getCanonicalName()
        );
    }

    @Override
    public void push(
        String topic,
        EventStatus status,
        Object payload,
        String eventName
    ) {
        pushEvent(
            null,
            topic,
            status,
            payload,
            eventName
        );
    }

    private void pushEvent(
        String id,
        String topic,
        EventStatus status,
        Object payload,
        String eventName
    ) {
        try {
            String eventJson = payload instanceof String ? (String)payload : objectMapper.writeValueAsString(payload);

            Event event;

            if (id != null) {
                event = new Event(
                    id,
                    eventName,
                    eventJson,
                    status
                );
            } else {
                event = new Event(
                    eventName,
                    eventJson,
                    status
                );
            }

            kafkaTemplate.send(topic, event).get();
        } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
