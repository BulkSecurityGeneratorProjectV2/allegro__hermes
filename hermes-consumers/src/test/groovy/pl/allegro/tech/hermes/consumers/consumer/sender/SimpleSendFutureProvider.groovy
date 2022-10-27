package pl.allegro.tech.hermes.consumers.consumer.sender

import spock.lang.Shared

import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import java.util.function.Function

class SimpleSendFutureProvider implements SendFutureProvider{
    @Override
    def <T extends MessageSendingResult> CompletableFuture<T> provide(Consumer<CompletableFuture<T>> resultFutureConsumer, Function<Throwable, T> exceptionMapper) {
        CompletableFuture<T> cf = new CompletableFuture<>()
        resultFutureConsumer.accept(cf)
        return cf
    }
}
