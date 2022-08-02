package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import com.codahale.metrics.Gauge;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.management.OperatingSystemMXBean;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadLimiter;
import pl.allegro.tech.hermes.tracker.consumers.MessageMetadata;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class NegotiatedLoadLimiter implements LoadLimiter {

    private static final Logger logger = LoggerFactory.getLogger(NegotiatedLoadLimiter.class);

    private final OperatingSystemMXBean platformMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    private final RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    private final RateLimiter rateLimiter;
    private final double cpuLimit;
    private final Duration limitsAdjustingInterval;
    private final HermesMetrics hermesMetrics;
    private final NegotiatedWorkloadMetricsRegistry workloadMetricsRegistry;
    private final ScheduledExecutorService executor;
    private final MessagesCounter counter = new MessagesCounter();
    private final Map<SubscriptionName, MessagesCounter> subscriptionMessageCounters = new ConcurrentHashMap<>();

    private volatile long lastProcessCpuTime = 0;
    private volatile long lastReset = 0;

    private final LongAdder timer = new LongAdder();
    private final Map<SubscriptionName, Long> lags = new ConcurrentHashMap<>();
    private final Map<String, Long> timestamps = new ConcurrentHashMap<>();
    private final LongAdder processingQueueSize = new LongAdder();

    public NegotiatedLoadLimiter(double cpuLimit,
                                 Duration limitsAdjustingInterval,
                                 HermesMetrics hermesMetrics,
                                 NegotiatedWorkloadMetricsRegistry workloadMetricsRegistry) {
        this.cpuLimit = cpuLimit;
        this.limitsAdjustingInterval = limitsAdjustingInterval;
        this.hermesMetrics = hermesMetrics;
        this.workloadMetricsRegistry = workloadMetricsRegistry;
        this.rateLimiter = RateLimiter.create(100);
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("workload-limits-adjuster-%d").build()
        );
        hermesMetrics.registerGauge("consumers-workload.limit", (Gauge<Double>) rateLimiter::getRate);
        hermesMetrics.registerGauge("consumers-workload.queue", (Gauge<Long>) processingQueueSize::sum);
        hermesMetrics.registerGauge("consumers-workload.time-queue", (Gauge<Integer>) timestamps::size);
        hermesMetrics.registerGauge("consumers-workload.timer", new Gauge<Long>() {
            @Override
            public Long getValue() {
                long now = System.nanoTime();
                long sum = 0;
                for (Long start : timestamps.values()) {
                    long elapsed = now - start;

                }
                return timestamps.values().stream().mapToLong(l -> now - l).sum();
            }
        });

        hermesMetrics.registerGauge("consumers-workload.lag", new Gauge<Long>() {
            @Override
            public Long getValue() {
                long sum = lags.values().stream().mapToLong(l -> l).sum();
                return sum;
            }
        });
    }

    public void start() {
        executor.scheduleWithFixedDelay(this::adjustLimits, 0, limitsAdjustingInterval.getSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdown();
    }

    @Override
    public void acquireOut(SubscriptionName qualifiedName) {
        rateLimiter.acquire();
//        counter.recordOut(1);
//        getMessageCounter(qualifiedName).recordOut(1);
    }

    @Override
    public void acquireIn(SubscriptionName qualifiedName, int count) {

//        counter.recordIn(count);
//        getMessageCounter(qualifiedName).recordIn(count);
    }

    private void acquire(int count) {
        double acquire = rateLimiter.acquire(count);
        double ms = acquire * 1000;
        hermesMetrics.sleepHistogram().update((long) ms);
    }

    @Override
    public void reportLag(SubscriptionName qualifiedName, Map<TopicPartition, Long> topicPartitionLongMap) {
        long lag = topicPartitionLongMap.values().stream().mapToLong(l -> l).sum();
        lags.put(qualifiedName, lag);
    }

    @Override
    public void addToTimeQueue(Message message) {
        addToTimeQueue(message.getTopic() + "$" + message.getSubscription() + "$" + message.getId());
    }

    @Override
    public void removeFromTimeQueue(Message message) {
        removeFromTimeQueue(message.getTopic() + "$" + message.getSubscription() + "$" + message.getId());
    }

    private void removeFromTimeQueue(String messageId)  {
        processingQueueSize.add(-1);
        Long start = timestamps.remove(messageId);
        if (start != null) {
            long now = System.nanoTime();
            long elapsed = now - start;
            hermesMetrics.timeHistogram().update(elapsed);
            timer.add(elapsed);
        }
    }

    private void addToTimeQueue(String messageId) {
        processingQueueSize.add(1);
        timestamps.put(messageId, System.nanoTime());
        acquire(1);
    }

    @Override
    public void addToTimeQueue(MessageBatch batch) {
        if (batch.getRetryCounter() > 0) {
            for (MessageMetadata message : batch.getMessagesMetadata()) {
                addToTimeQueue(message.getTopic() + "$" + message.getSubscription() + "$" + message.getMessageId());
            }
        }
    }

    @Override
    public void removeFromTimeQueue(MessageBatch batch) {
        for (MessageMetadata message : batch.getMessagesMetadata()) {
            removeFromTimeQueue(message.getTopic() + "$" + message.getSubscription() + "$" + message.getMessageId());
        }
    }

    @Override
    public void acquire() {
        double acquire = rateLimiter.acquire();
        double ms = acquire * 1000;
        hermesMetrics.sleepHistogram().update((long) ms);
    }

    private MessagesCounter getMessageCounter(SubscriptionName subscriptionName) {
        return subscriptionMessageCounters.computeIfAbsent(subscriptionName, ignore -> new MessagesCounter());
    }

    private void adjustLimits() {
        long uptime = runtimeMxBean.getUptime();
        long elapsedMillis = uptime - lastReset;
        lastReset = uptime;

        double currentCpuUtilization = calculateCpuUtilization(elapsedMillis);
//        Throughput throughput = counter.calculateThroughput(elapsedMillis);
        double limit = rateLimiter.getRate();
        double newLimit = limit * cpuLimit / currentCpuUtilization;
        rateLimiter.setRate(newLimit);
    }

    private boolean isBusy(Throughput throughput) {
        double limit = rateLimiter.getRate();
        double current = throughput.sum();
        return current / limit > 0.9;
    }

    private double calculateCpuUtilization(long elapsedMillis) {
        long elapsedNanoseconds = TimeUnit.MILLISECONDS.toNanos(elapsedMillis);
        long processCpuTime = platformMXBean.getProcessCpuTime();
        double cpuUtilization = (processCpuTime - lastProcessCpuTime) / (double) elapsedNanoseconds;
        lastProcessCpuTime = processCpuTime;
        return cpuUtilization;
    }

    private static class MessagesCounter {
        private final LongAdder messagesIn = new LongAdder();
        private final LongAdder messagesOut = new LongAdder();

        void recordIn(int count) {
            messagesIn.add(count);
        }

        void recordOut(int count) {
            messagesOut.add(count);
        }

        Throughput calculateThroughput(long elapsedMillis) {
            long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedMillis);
            long period = Math.max(elapsedSeconds, 1);
            return new Throughput(
                    (double) messagesIn.sumThenReset() / period,
                    (double) messagesOut.sumThenReset() / period
            );
        }
    }
}
