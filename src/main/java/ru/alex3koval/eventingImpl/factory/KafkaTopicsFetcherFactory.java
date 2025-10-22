package ru.alex3koval.eventingImpl.factory;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class KafkaTopicsFetcherFactory {
    private final AdminClient client;

    public CompletableFuture<Set<String>> create() {
        return client.listTopics().names().toCompletionStage().toCompletableFuture();
    }
}
