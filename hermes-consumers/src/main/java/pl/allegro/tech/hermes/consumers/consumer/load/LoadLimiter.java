package pl.allegro.tech.hermes.consumers.consumer.load;

import org.apache.kafka.common.TopicPartition;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;

import java.util.Map;

public interface LoadLimiter {

    void acquireOut(SubscriptionName qualifiedName);

    void acquireIn(SubscriptionName qualifiedName, int count);

//    void removeFromProcessingQueue(SubscriptionName subscriptionName, int size);
//
//    void addToProcessingQueue(SubscriptionName qualifiedName, int size);

    void reportLag(SubscriptionName qualifiedName, Map<TopicPartition, Long> topicPartitionLongMap);

    void addToTimeQueue(Message messageId);

    void removeFromTimeQueue(Message id);

    void addToTimeQueue(MessageBatch batch);

    void removeFromTimeQueue(MessageBatch batch);

    void acquire(String operation);
}
