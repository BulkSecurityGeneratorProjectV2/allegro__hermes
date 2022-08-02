package pl.allegro.tech.hermes.consumers.consumer;

import org.apache.kafka.common.TopicPartition;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadLimiter;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;
import pl.allegro.tech.hermes.consumers.consumer.load.SubscriptionLoadReporter;

import java.util.Map;

public class NoOpSubscriptionLoadReporter implements SubscriptionLoadReporter, LoadLimiter {
    @Override
    public void recordStatus(SubscriptionName subscriptionName, LoadStatus loadStatus) {

    }

    @Override
    public void recordMessagesOut(SubscriptionName subscriptionName, int count) {

    }

    @Override
    public void recordMessagesIn(SubscriptionName subscriptionName, int count) {

    }

    @Override
    public void acquireOut(SubscriptionName qualifiedName) {

    }

    @Override
    public void acquireIn(SubscriptionName qualifiedName, int count) {

    }
//
//    @Override
//    public void removeFromProcessingQueue(SubscriptionName subscriptionName, int size) {
//
//    }
//
//    @Override
//    public void addToProcessingQueue(SubscriptionName qualifiedName, int size) {
//
//    }

    @Override
    public void reportLag(SubscriptionName qualifiedName, Map<TopicPartition, Long> topicPartitionLongMap) {

    }

    @Override
    public void addToTimeQueue(Message messageId) {

    }

    @Override
    public void removeFromTimeQueue(Message id) {

    }

    @Override
    public void addToTimeQueue(MessageBatch batch) {

    }

    @Override
    public void removeFromTimeQueue(MessageBatch batch) {

    }

    @Override
    public void acquire() {

    }
}
