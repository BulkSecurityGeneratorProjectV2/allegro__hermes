package pl.allegro.tech.hermes.consumers.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Collection;

public class OffsetCommitterConsumerRebalanceListener implements ConsumerRebalanceListener {

    private final SubscriptionName name;
    private final ConsumerPartitionAssignmentState state;

    public OffsetCommitterConsumerRebalanceListener(SubscriptionName name, ConsumerPartitionAssignmentState state) {
        this.name = name;
        this.state = state;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        state.revoke(name, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        state.assign(name, partitions);
    }
}
