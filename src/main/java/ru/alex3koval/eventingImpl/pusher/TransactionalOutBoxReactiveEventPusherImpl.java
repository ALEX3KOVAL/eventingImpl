package ru.alex3koval.eventingImpl.pusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.dto.CreateEventWDTO;
import ru.alex3koval.eventingContract.vo.EventStatus;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.Function;

@RequiredArgsConstructor
public class TransactionalOutBoxReactiveEventPusherImpl<T> implements ReactiveEventPusher<T> {
    private final ObjectMapper mapper;
    private final Function<CreateEventWDTO, Mono<T>> pushInDbFunction;

    @Override
    public Mono<T> push(String topic, EventStatus eventStatus, Object payload) {
        if (payload instanceof String) {
            throw new RuntimeException("Payload имеет тип String, но не передано имя события");
        }

        return pushEvent(
            topic,
            eventStatus,
            payload,
            payload.getClass().getCanonicalName()
        );
    }

    @Override
    public Mono<T> push(String topic, EventStatus eventStatus, Object payload, String eventName) {
        return pushEvent(
            topic,
            eventStatus,
            payload,
            eventName
        );
    }

    private Mono<T> pushEvent(
        String topic,
        EventStatus status,
        Object payload,
        String eventName
    ) {
        try {
            return pushInDbFunction.apply(
                new CreateEventWDTO(
                    eventName,
                    topic,
                    serializeToJson(mapper, payload),
                    status,
                    LocalDateTime.now(ZoneOffset.UTC)
                )
            );
        } catch (JsonProcessingException exc) {
            throw new RuntimeException(exc);
        }
    }
}
