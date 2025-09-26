package ru.alex3koval.eventingImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.alex3koval.eventingContract.Event;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.SyncEventPusher;
import ru.alex3koval.eventingContract.dto.CreateEventWDTO;
import ru.alex3koval.eventingContract.dto.EventRDTO;
import ru.alex3koval.eventingImpl.factory.KafkaTopicsFetcherFactory;
import ru.alex3koval.eventingImpl.pusher.AsyncTransactionalOutBoxEventPusherImpl;
import ru.alex3koval.eventingImpl.pusher.EventPusherImpl;
import ru.alex3koval.eventingImpl.pusher.SyncEventPusherImpl;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class EventingConfiguration {
    @Bean
    KafkaTemplate<String, Event> kafkaTemplate(
        ProducerFactory<String, Event> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @Qualifier("reactiveEventPusher")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    ReactiveEventPusher<Void> eventPusher(StreamBridge streamBridge, ObjectMapper objectMapper) {
        return new EventPusherImpl<>(streamBridge, objectMapper);
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    SyncEventPusher syncEventPusher(StreamBridge streamBridge, ObjectMapper objectMapper) {
        return new SyncEventPusherImpl(streamBridge, objectMapper);
    }

    @Bean
    @Qualifier("transactionalOutboxReactivePusher")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    ReactiveEventPusher<Long> transactionalOutboxAsyncEventPusher(
        @Qualifier("asyncTransactionalOutBoxPushingFunction")
        Function<CreateEventWDTO, Mono<Long>> pushInDbFunction,
        ObjectMapper mapper
    ) {
        return new AsyncTransactionalOutBoxEventPusherImpl<>(mapper, pushInDbFunction);
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
