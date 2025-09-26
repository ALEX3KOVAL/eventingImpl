package ru.alex3koval.eventingImpl.factory;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import reactor.core.publisher.Mono;

import java.util.Set;

@RequiredArgsConstructor
public class KafkaTopicsFetcherFactory {
    private final AdminClient client;

    public Mono<Set<String>> create() {
        return Mono.fromFuture(
            client.listTopics().names().toCompletionStage().toCompletableFuture()
        );
    }
}
