package ru.alex3koval.eventingImpl.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.EventListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
public class EventListenerManager {
    private final ConcurrentKafkaListenerContainerFactory<String, Event> containerFactory;
    private final ObjectMapper objectMapper;

    private final Map<String, ConcurrentMessageListenerContainer<String, Event>> containers =
        new ConcurrentHashMap<>();

    public <T, R> void registerListener(
        String topic,
        String groupId,
        EventListener<T, R> eventListener,
        Class<T> payloadClazz
    ) {
        ConcurrentMessageListenerContainer<String, Event> container = containerFactory.createContainer(topic);
        ContainerProperties containerProps = container.getContainerProperties();

        containerProps.setMessageListener((MessageListener<String, Event>) record -> {
            try {
                T payload = objectMapper.readValue(record.value().getJson(), payloadClazz);
                eventListener.onEvent(payload);
            } catch (JsonProcessingException e) {
                log.error(e.getMessage(), e);
            }
        });
        containerProps.setGroupId(groupId);

        container.start();
        containers.put(topic, container);
    }
}
