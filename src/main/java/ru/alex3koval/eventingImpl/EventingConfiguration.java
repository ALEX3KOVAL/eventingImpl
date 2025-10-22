package ru.alex3koval.eventingImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.*;
import reactor.core.scheduler.Schedulers;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.SyncEventPusher;
import ru.alex3koval.eventingContract.dto.EventRDTO;
import ru.alex3koval.eventingImpl.factory.KafkaTopicsFetcherFactory;
import ru.alex3koval.eventingImpl.pusher.EventPusherImpl;
import ru.alex3koval.eventingImpl.pusher.SyncEventPusherImpl;
import ru.alex3koval.eventingImpl.serialization.EventDeserializer;
import ru.alex3koval.eventingImpl.serialization.EventSerializer;

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
    ProducerFactory<String, Event> producerFactory(
        EventSerializer eventSerializer,
        KafkaProperties kafkaProperties
    ) {
        return new DefaultKafkaProducerFactory<>(
            kafkaProperties.buildProducerProperties(),
            new StringSerializer(),
            eventSerializer
        );
    }

    @Bean
    ConsumerFactory<String, Event> consumerFactory(
        KafkaProperties kafkaProperties,
        EventDeserializer eventDeserializer
    ) {
        return new DefaultKafkaConsumerFactory<>(
            kafkaProperties.buildConsumerProperties(),
            new StringDeserializer(),
            eventDeserializer
        );
    }

    @Bean
    EventSerializer eventSerializer(ObjectMapper objectMapper) {
        return new EventSerializer(objectMapper);
    }

    @Bean
    EventDeserializer eventDeserializer(ObjectMapper objectMapper) {
        return new EventDeserializer(objectMapper);
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

    @Bean("reactiveEventPusher")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    ReactiveEventPusher<Void> eventPusher(KafkaTemplate<String, Event> kafkaTemplate, ObjectMapper objectMapper) {
        return new EventPusherImpl(kafkaTemplate, objectMapper);
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    SyncEventPusher syncEventPusher(KafkaTemplate<String, Event> streamBridge, ObjectMapper objectMapper) {
        return new SyncEventPusherImpl(streamBridge, objectMapper);
    }

    @Bean("pushEventConsumer")
    Consumer<EventRDTO> pushEventConsumer(
        @Qualifier("reactiveEventPusher") ReactiveEventPusher<Void> pusher
    ) {
        return rdto -> pusher
            .push(
                rdto.id(),
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
