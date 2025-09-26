package ru.alex3koval.eventingImpl.pusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import reactor.core.publisher.Mono;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.vo.EventStatus;
import ru.alex3koval.eventingImpl.exception.SendingFailedException;

@RequiredArgsConstructor
public class EventPusherImpl<T> implements ReactiveEventPusher<T> {
    private final StreamBridge streamBridge;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<T> push(String topic, EventStatus status, Object payload) {
        if (payload instanceof String) {
            throw new RuntimeException("Payload имеет тип String, но не передано имя события");
        }

        return pushEvent(
            topic,
            status,
            payload,
            payload.getClass().getCanonicalName()
        );
    }

    @Override
    public Mono<T> push(String topic, EventStatus status, Object payload, String eventName) {
        return pushEvent(
            topic,
            status,
            payload,
            eventName
        );
    }

    private Mono<T> pushEvent(String topic, EventStatus status, Object payload, String eventName) {
        try {
            String eventJson = payload instanceof String ? (String)payload : objectMapper.writeValueAsString(payload);

            return Mono
                .fromCallable(() ->
                    streamBridge.send(
                        topic,
                        new Event(
                            eventName,
                            eventJson,
                            status
                        )
                    )
                )
                .flatMap(sent -> {
                    if (sent) {
                        return Mono.empty();
                    }

                    return Mono.error(
                        new SendingFailedException("Не удалось отправить событие: " + eventJson)
                    );
                });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
