package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

public class ZookeeperNegotiatedWorkloadMetricsRegistry implements NegotiatedWorkloadMetricsRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperNegotiatedWorkloadMetricsRegistry.class);

    private final CuratorFramework curator;
    private final ZookeeperPaths zookeeperPaths;
    private final String basePath;
    private final String currentConsumerPath;

    public ZookeeperNegotiatedWorkloadMetricsRegistry(CuratorFramework curator,
                                            SubscriptionIds subscriptionIds,
                                            ZookeeperPaths zookeeperPaths,
                                            String currentConsumerId,
                                            String clusterName,
                                            int sbeEncoderBufferSize) {
        this.curator = curator;
        this.zookeeperPaths = zookeeperPaths;
        this.basePath = zookeeperPaths.join(zookeeperPaths.basePath(), "consumers-workload-experiment", clusterName, "metrics");
        this.currentConsumerPath = zookeeperPaths.join(basePath, currentConsumerId);
    }

}
