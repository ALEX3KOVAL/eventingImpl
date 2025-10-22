package ru.alex3koval.eventingImpl.pusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.vo.EventStatus;
import ru.alex3koval.eventingImpl.exception.SendingFailedException;

@RequiredArgsConstructor
public class EventPusherImpl implements ReactiveEventPusher<Void> {
    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> push(
        String id,
        String topic,
        EventStatus status,
        Object payload
    ) {
        if (payload instanceof String) {
            throw new RuntimeException("Payload имеет тип String, но не передано имя события");
        }

        return pushEvent(
            id,
            topic,
            status,
            payload,
            payload.getClass().getCanonicalName()
        );
    }

    @Override
    public Mono<Void> push(
        String id,
        String topic,
        EventStatus status,
        Object payload,
        String eventName
    ) {
        return pushEvent(
            id,
            topic,
            status,
            payload,
            eventName
        );
    }

    @Override
    public Mono<Void> push(String topic, EventStatus eventStatus, Object payload) {
        return null;
    }

    @Override
    public Mono<Void> push(String topic, EventStatus eventStatus, Object payload, String eventName) {
        return null;
    }

    private Mono<Void> pushEvent(
        String id,
        String topic,
        EventStatus status,
        Object payload,
        String eventName
    ) {
        try {
            String eventJson = payload instanceof String ? (String)payload : objectMapper.writeValueAsString(payload);

            Event event = new Event(
                id,
                eventName,
                eventJson,
                status
            );

            return Mono
                .fromFuture(
                    kafkaTemplate.send(
                        topic,
                        event
                    )
                )
                .onErrorResume(exc ->
                    Mono.error(new SendingFailedException("Не удалось отправить событие: " + eventJson))
                )
                .then();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
