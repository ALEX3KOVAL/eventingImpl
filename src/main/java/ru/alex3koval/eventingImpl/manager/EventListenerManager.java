package ru.alex3koval.eventingImpl.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.*;
import reactor.core.publisher.Mono;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.EventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class EventListenerManager {
    private final ConcurrentKafkaListenerContainerFactory<String, Event> containerFactory;
    private final ObjectMapper objectMapper;
    private final Function<String, Mono<?>> onEventHasBeenProcessed;

    private final Map<String, ConcurrentMessageListenerContainer<String, Event>> containers =
        new ConcurrentHashMap<>();
    private final Map<String, List<EventListener<?, ?>>> handlers = new ConcurrentHashMap<>();

    public void stopContainers() {
        containers
            .values()
            .forEach(AbstractMessageListenerContainer::stop);
    }

    public <T> void registerListener(
        String topic,
        String groupId,
        EventListener<T, Mono<?>> eventListener,
        Class<T> payloadClazz
    ) {
        String key = String.format("%s-%s-%s", topic, payloadClazz.getCanonicalName(), groupId);

        handlers
            .computeIfAbsent(key, k -> new ArrayList<>())
            .add(eventListener);

        ConcurrentMessageListenerContainer<String, Event> container = containers.computeIfAbsent(topic, topicEl -> {
            ConcurrentMessageListenerContainer<String, Event> containerEl = containerFactory.createContainer(topic);
            ContainerProperties containerProps = containerEl.getContainerProperties();

            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
            containerProps.setGroupId(groupId);
            containerProps.setMessageListener((AcknowledgingMessageListener<String, Event>) (record, ack) -> {
                try {
                    T payload = objectMapper.readValue(record.value().getJson(), payloadClazz);

                    handlers
                        .get(key)
                        .forEach(listener ->
                            ((EventListener<T, Mono<?>>) listener)
                                .onEvent(payload)
                                .flatMap(handlerResult ->
                                    onEventHasBeenProcessed.apply(record.value().getId())
                                )
                                .flatMap(eventId -> {
                                    try {
                                        Objects.requireNonNull(ack).acknowledge();
                                        return Mono.empty();
                                    } catch (Exception exc) {
                                        return Mono.error(exc);
                                    }
                                })
                                .doOnError(err -> log.error("Ошибка обработки события", err))
                                .subscribe()
                        );
                } catch (JsonProcessingException e) {
                    log.error(e.getMessage(), e);
                }
            });

            return containerEl;
        });

        CompletableFuture.runAsync(container::start);
    }
}
