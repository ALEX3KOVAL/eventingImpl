package ru.alex3koval.eventingImpl.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;
import ru.alex3koval.eventingContract.ReactiveEventPusher;
import ru.alex3koval.eventingContract.dto.CreateEventWDTO;
import ru.alex3koval.eventingImpl.pusher.TransactionalOutBoxReactiveEventPusherImpl;

import java.util.function.Function;

@RequiredArgsConstructor
public class TransactionalOutBoxReactiveEventPusherFactory<T> {
    private final Function<CreateEventWDTO, Mono<T>> pushInDbFunction;
    private final ObjectMapper mapper;

    public ReactiveEventPusher<T> create() {
        return new TransactionalOutBoxReactiveEventPusherImpl<>(
            mapper,
            pushInDbFunction
        );
    }
}
