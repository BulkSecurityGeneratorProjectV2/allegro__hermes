package pl.allegro.tech.hermes.consumers.consumer.sender;

import pl.allegro.tech.hermes.consumers.consumer.rate.ConsumerRateLimiter;
import pl.allegro.tech.hermes.consumers.consumer.sender.timeout.FutureAsyncTimeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

// provides futures which are rate limited and completed exceptionally after given timeout
public class ThrottlingSendFutureProvider implements SendFutureProvider{
    private final ConsumerRateLimiter rateLimiter;
    private final List<Predicate<MessageSendingResult>> ignore;
    private final FutureAsyncTimeout async;
    private final int requestTimeoutMs;
    private final int asyncTimeoutMs;

    public ThrottlingSendFutureProvider(ConsumerRateLimiter rateLimiter,
                                        List<Predicate<MessageSendingResult>> ignore,
                                        FutureAsyncTimeout async,
                                        int requestTimeoutMs,
                                        int asyncTimeoutMs) {
        this.rateLimiter = rateLimiter;
        this.ignore = ignore;
        this.async = async;
        this.requestTimeoutMs = requestTimeoutMs;
        this.asyncTimeoutMs = asyncTimeoutMs;
    }

    public <T extends MessageSendingResult> CompletableFuture<T> provide(Consumer<CompletableFuture<T>> resultFutureConsumer, Function<Throwable, T> exceptionMapper) {
        try {
            rateLimiter.acquire();
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletableFuture<T> withTimeout = async.within(
                    resultFuture,
                    Duration.ofMillis(asyncTimeoutMs + requestTimeoutMs),
                    exceptionMapper);
            resultFutureConsumer.accept(withTimeout);
            return whenComplete(withTimeout, exceptionMapper);
        } catch (Exception e) {
            rateLimiter.registerFailedSending();
            return CompletableFuture.completedFuture(exceptionMapper.apply(e));
        }
    }

    private <T extends MessageSendingResult> CompletableFuture<T> whenComplete(CompletableFuture<T> future, Function<Throwable, T> exceptionMapper) {
        return future.handle((result, throwable) -> {
            if (throwable != null) {
                rateLimiter.registerFailedSending();
                return exceptionMapper.apply(throwable);
            } else {
                if (result.succeeded()) {
                    rateLimiter.registerSuccessfulSending();
                } else {
                    registerResultInRateLimiter(result);
                }
                return result;
            }
        });
    }

    private void registerResultInRateLimiter(MessageSendingResult result) {
        if (ignore.stream().anyMatch(p -> p.test(result))) {
            rateLimiter.registerSuccessfulSending();
        } else {
            rateLimiter.registerFailedSending();
        }
    }


}
