package pl.allegro.tech.hermes.consumers.consumer.sender;

import pl.allegro.tech.hermes.consumers.consumer.Message;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public abstract class CompletableFutureAwareMessageSender implements MessageSender {

    private final Function<Throwable, MessageSendingResult> exceptionMapper = MessageSendingResult::failedResult;

    @Override
    public CompletableFuture<MessageSendingResult> send(Message message, SendFutureProvider sendFutureProvider) {
        return sendFutureProvider.provide(
                cf -> sendMessage(message, cf),
                exceptionMapper
        );
    }

    protected abstract void sendMessage(Message message, CompletableFuture<MessageSendingResult> resultFuture);
}
