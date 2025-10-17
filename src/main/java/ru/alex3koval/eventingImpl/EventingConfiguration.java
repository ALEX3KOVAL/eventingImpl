package ru.alex3koval.eventingImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import reactor.core.scheduler.Schedulers;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.SyncEventPusher;
import ru.alex3koval.eventingContract.dto.EventRDTO;
import ru.alex3koval.eventingImpl.factory.KafkaTopicsFetcherFactory;
import ru.alex3koval.eventingImpl.pusher.EventPusherImpl;
import ru.alex3koval.eventingImpl.pusher.SyncEventPusherImpl;
import ru.alex3koval.eventingImpl.serialization.EventSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@Configuration
public class EventingConfiguration {
    @Bean
    KafkaTemplate<String, Event> kafkaTemplate(
        ProducerFactory<String, Event> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    ProducerFactory<String, Event> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    EventSerializer eventSerializer(ObjectMapper objectMapper) {
        return new EventSerializer(objectMapper);
    }

    @Bean
    KafkaAdmin kafkaAdmin(KafkaProperties kafkaProperties) {
        return new KafkaAdmin(
            kafkaProperties.buildAdminProperties(null)
        );
    }

    @Bean
    AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    @Qualifier("reactiveEventPusher")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    ReactiveEventPusher<Void> eventPusher(KafkaTemplate<String, Event> kafkaTemplate, ObjectMapper objectMapper) {
        return new EventPusherImpl(kafkaTemplate, objectMapper);
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    SyncEventPusher syncEventPusher(KafkaTemplate<String, Event> streamBridge, ObjectMapper objectMapper) {
        return new SyncEventPusherImpl(streamBridge, objectMapper);
    }

    @Bean
    @Qualifier("pushEventConsumer")
    Consumer<EventRDTO> pushEventConsumer(
        @Qualifier("reactiveEventPusher") ReactiveEventPusher<Void> pusher
    ) {
        return rdto -> pusher
            .push(
                rdto.topic(),
                rdto.status(),
                rdto.json(),
                rdto.name()
            )
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe();
    }

    @Bean
    KafkaTopicsFetcherFactory allTopics(AdminClient kafkaAdminClient) {
        return new KafkaTopicsFetcherFactory(kafkaAdminClient);
    }
}
