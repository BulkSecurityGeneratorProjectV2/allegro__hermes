package pl.allegro.tech.hermes.consumers.supervisor.workload.weighted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.supervisor.workload.SubscriptionAssignment;
import pl.allegro.tech.hermes.consumers.supervisor.workload.SubscriptionAssignmentView;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkBalancer;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkBalancingResult;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkloadConstraints;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static pl.allegro.tech.hermes.consumers.supervisor.workload.weighted.ConsumerNode.LIGHTEST_CONSUMER_FIRST;
import static pl.allegro.tech.hermes.consumers.supervisor.workload.weighted.ConsumerTask.HEAVIEST_TASK_FIRST;

public class WeightedWorkBalancer implements WorkBalancer {

    private static final Logger logger = LoggerFactory.getLogger(WeightedWorkBalancer.class);

    private final Clock clock;
    private final Duration stabilizationWindowSize;
    private final double minSignificantChangePercent;
    private final SubscriptionProfileProvider subscriptionProfileProvider;

    public WeightedWorkBalancer(Clock clock,
                                Duration stabilizationWindowSize,
                                double minSignificantChangePercent,
                                SubscriptionProfileProvider subscriptionProfileProvider) {
        this.clock = clock;
        this.stabilizationWindowSize = stabilizationWindowSize;
        this.minSignificantChangePercent = minSignificantChangePercent;
        this.subscriptionProfileProvider = subscriptionProfileProvider;
    }

    @Override
    public WorkBalancingResult balance(List<SubscriptionName> subscriptions,
                                       List<String> activeConsumerNodes,
                                       SubscriptionAssignmentView currentState,
                                       WorkloadConstraints constraints) {
        AssignmentPlan initialPlan = restoreValidAssignments(subscriptions, activeConsumerNodes, currentState, constraints);
        AssignmentPlan planWithAllPossibleAssignments = tryAssignUnassignedTasks(initialPlan);
        AssignmentPlan finalPlan = rebalance(planWithAllPossibleAssignments);
        return finalPlan.toWorkBalancingResult();
    }

    private AssignmentPlan restoreValidAssignments(List<SubscriptionName> subscriptions,
                                                   List<String> activeConsumerNodes,
                                                   SubscriptionAssignmentView currentState,
                                                   WorkloadConstraints constraints) {
        Map<String, ConsumerNode> consumers = createConsumers(activeConsumerNodes, constraints);
        List<ConsumerTask> unassignedTasks = new ArrayList<>();
        for (SubscriptionName subscriptionName : subscriptions) {
            SubscriptionProfile subscriptionProfile = subscriptionProfileProvider.get(subscriptionName);
            Queue<ConsumerTask> consumerTasks = createConsumerTasks(subscriptionName, subscriptionProfile, constraints);
            Set<String> consumerNodesForSubscription = currentState.getConsumerNodesForSubscription(subscriptionName);
            for (String consumerId : consumerNodesForSubscription) {
                ConsumerNode consumerNode = consumers.get(consumerId);
                if (consumerNode != null && !consumerTasks.isEmpty()) {
                    ConsumerTask consumerTask = consumerTasks.poll();
                    consumerNode.assign(consumerTask);
                }
            }
            unassignedTasks.addAll(consumerTasks);
        }
        return new AssignmentPlan(unassignedTasks, consumers.values());
    }

    private Map<String, ConsumerNode> createConsumers(List<String> activeConsumerNodes, WorkloadConstraints constraints) {
        return activeConsumerNodes.stream()
                .map(consumerId -> new ConsumerNode(consumerId, constraints.getMaxSubscriptionsPerConsumer()))
                .collect(toMap(ConsumerNode::getConsumerId, Function.identity()));
    }

    private Queue<ConsumerTask> createConsumerTasks(SubscriptionName subscriptionName,
                                                    SubscriptionProfile subscriptionProfile,
                                                    WorkloadConstraints constraints) {
        int consumerCount = constraints.getConsumerCount(subscriptionName);
        return IntStream.range(0, consumerCount)
                .mapToObj(ignore -> new ConsumerTask(subscriptionName, subscriptionProfile))
                .collect(toCollection(ArrayDeque::new));
    }

    private AssignmentPlan tryAssignUnassignedTasks(AssignmentPlan assignmentPlan) {
        PriorityQueue<ConsumerTask> tasksToAssign = new PriorityQueue<>(HEAVIEST_TASK_FIRST);
        tasksToAssign.addAll(assignmentPlan.getUnassignedTasks());
        List<ConsumerTask> unassignedTasks = new ArrayList<>();
        while (!tasksToAssign.isEmpty()) {
            ConsumerTask consumerTask = tasksToAssign.poll();
            Optional<ConsumerNode> candidate = selectConsumerNode(consumerTask, assignmentPlan.getConsumers());
            if (candidate.isPresent()) {
                candidate.get().assign(consumerTask);
            } else {
                unassignedTasks.add(consumerTask);
            }
        }
        return new AssignmentPlan(unassignedTasks, assignmentPlan.getConsumers());
    }

    private Optional<ConsumerNode> selectConsumerNode(ConsumerTask consumerTask, Collection<ConsumerNode> consumers) {
        return consumers.stream()
                .filter(consumerNode -> consumerNode.isNotAssigned(consumerTask))
                .filter(consumerNode -> !consumerNode.isFull())
                .min(LIGHTEST_CONSUMER_FIRST);
    }

    private AssignmentPlan rebalance(AssignmentPlan plan) {
        Collection<ConsumerNode> consumers = plan.getConsumers();
        TargetConsumerLoad targetLoad = calculateTargetConsumerLoad(consumers);
        List<ConsumerNode> overloadedConsumers = consumers.stream()
                .filter(consumerNode -> isOverloaded(consumerNode, targetLoad))
                .collect(toList());
        for (ConsumerNode overloaded : overloadedConsumers) {
            List<ConsumerNode> underloadedConsumers = consumers.stream()
                    .filter(consumer -> !consumer.equals(overloaded))
                    .filter(consumerNode -> isUnderloaded(consumerNode, targetLoad))
                    .collect(toList());
            for (ConsumerNode underloaded : underloadedConsumers) {
                tryMoveOutTasksFromOverloaded(overloaded, underloaded, targetLoad);
                trySwapTasks(overloaded, underloaded, targetLoad);
                if (isBalanced(overloaded, targetLoad)) {
                    break;
                }
            }
        }
        for (ConsumerNode consumerNode : consumers) {
            logger.debug("Consumer {} weight after rebalance: {}", consumerNode, consumerNode.getWeight());
        }
        return new AssignmentPlan(plan.getUnassignedTasks(), consumers);
    }

    private TargetConsumerLoad calculateTargetConsumerLoad(Collection<ConsumerNode> consumers) {
        int totalNumberOfTasks = consumers.stream().mapToInt(ConsumerNode::getAssignedTaskCount).sum();
        int consumerCount = consumers.size();
        int maxNumberOfTasksPerConsumer = consumerCount == 0 ? 0 : (totalNumberOfTasks + consumerCount - 1) / consumerCount;
        Weight targetConsumerWeight = calculateTargetConsumerWeight(consumers);
        return new TargetConsumerLoad(targetConsumerWeight, maxNumberOfTasksPerConsumer);
    }

    private Weight calculateTargetConsumerWeight(Collection<ConsumerNode> consumers) {
        if (consumers.isEmpty()) {
            return Weight.ZERO;
        }
        Weight sum = consumers.stream()
                .map(ConsumerNode::getWeight)
                .reduce(Weight.ZERO, Weight::add);
        return sum.divide(consumers.size());
    }

    private void tryMoveOutTasksFromOverloaded(ConsumerNode overloaded, ConsumerNode underloaded, TargetConsumerLoad targetLoad) {
        List<ConsumerTask> candidatesToMoveOut = overloaded.getAssignedTasks().stream()
                .filter(underloaded::isNotAssigned)
                .collect(toList());
        ListIterator<ConsumerTask> consumerTasIterator = candidatesToMoveOut.listIterator();
        while (consumerTasIterator.hasNext() && hasTooManyTasks(overloaded, targetLoad) && !hasTooManyTasks(underloaded, targetLoad)) {
            ConsumerTask taskFromOverloaded = consumerTasIterator.next();
            MoveOutProposal proposal = new MoveOutProposal(overloaded, underloaded, taskFromOverloaded);
            if (isProfitable(proposal, targetLoad)) {
                overloaded.unassign(taskFromOverloaded);
                underloaded.assign(taskFromOverloaded);
            }
        }
    }

    private boolean hasTooManyTasks(ConsumerNode consumerNode, TargetConsumerLoad targetConsumerLoad) {
        return consumerNode.getAssignedTaskCount() >= targetConsumerLoad.getNumberOfTasks();
    }

    private boolean isProfitable(MoveOutProposal proposal, TargetConsumerLoad targetLoad) {
        if (targetLoad.getWeight().isGreaterThanOrEqualTo(proposal.getFinalUnderloadedWeight())) {
            logger.debug("MoveOut proposal will be applied:\n{}", proposal);
            return true;
        }
        return false;
    }

    private void trySwapTasks(ConsumerNode overloaded, ConsumerNode underloaded, TargetConsumerLoad targetLoad) {
        List<ConsumerTask> candidatesFromOverloaded = findCandidatesForMovingOut(overloaded, underloaded);
        List<ConsumerTask> candidatesFromUnderloaded = findCandidatesForMovingOut(underloaded, overloaded);
        for (ConsumerTask taskFromOverloaded : candidatesFromOverloaded) {
            ListIterator<ConsumerTask> candidatesFromUnderloadedIterator = candidatesFromUnderloaded.listIterator();
            while (candidatesFromUnderloadedIterator.hasNext()) {
                ConsumerTask taskFromUnderloaded = candidatesFromUnderloadedIterator.next();
                SwapProposal proposal = new SwapProposal(overloaded, underloaded, taskFromOverloaded, taskFromUnderloaded);
                if (isProfitable(proposal, targetLoad)) {
                    overloaded.swap(taskFromOverloaded, taskFromUnderloaded);
                    underloaded.swap(taskFromUnderloaded, taskFromOverloaded);
                    candidatesFromUnderloadedIterator.remove();
                }
                if (isBalanced(overloaded, targetLoad)) {
                    return;
                }
            }
        }
    }

    private List<ConsumerTask> findCandidatesForMovingOut(ConsumerNode source, ConsumerNode destination) {
        return source.getAssignedTasks().stream()
                .filter(destination::isNotAssigned)
                .filter(this::isStable)
                .collect(toList());
    }

    private boolean isStable(ConsumerTask task) {
        return task.getLastRebalanceTimestamp() == null
                || !task.getLastRebalanceTimestamp().plus(stabilizationWindowSize).isAfter(clock.instant());
    }

    private boolean isOverloaded(ConsumerNode consumerNode, TargetConsumerLoad targetLoad) {
        return consumerNode.getWeight().isGreaterThan(targetLoad.getWeight())
                || consumerNode.getAssignedTaskCount() > targetLoad.getNumberOfTasks();
    }

    private boolean isUnderloaded(ConsumerNode consumerNode, TargetConsumerLoad targetLoad) {
        return consumerNode.getWeight().isLessThan(targetLoad.getWeight())
                || consumerNode.getAssignedTaskCount() < targetLoad.getNumberOfTasks();
    }

    private boolean isBalanced(ConsumerNode consumerNode, TargetConsumerLoad targetLoad) {
        return consumerNode.getWeight().isEqualTo(targetLoad.getWeight())
                && consumerNode.getAssignedTaskCount() <= targetLoad.getNumberOfTasks();
    }

    private boolean isProfitable(SwapProposal proposal, TargetConsumerLoad targetLoad) {
        Weight initialOverloadedWeight = proposal.getOverloadedWeight();
        Weight finalOverloadedWeight = proposal.getFinalOverloadedWeight();
        Weight finalUnderloadedWeight = proposal.getFinalUnderloadedWeight();

        if (finalUnderloadedWeight.isGreaterThan(targetLoad.getWeight())) {
            return false;
        }
        if (finalOverloadedWeight.isGreaterThan(initialOverloadedWeight)) {
            return false;
        }
        if (initialOverloadedWeight.isEqualTo(Weight.ZERO)) {
            return false;
        }
        double percentageChange = Weight.calculatePercentageChange(initialOverloadedWeight, finalOverloadedWeight);
        if (percentageChange >= minSignificantChangePercent) {
            logger.debug("Swap proposal will be applied:\n{}", proposal);
            return true;
        }
        return false;
    }

    private static class AssignmentPlan {

        private final List<ConsumerTask> unassignedTasks;
        private final Collection<ConsumerNode> consumers;

        AssignmentPlan(List<ConsumerTask> unassignedTasks, Collection<ConsumerNode> consumers) {
            this.unassignedTasks = unassignedTasks;
            this.consumers = consumers;
        }

        Collection<ConsumerNode> getConsumers() {
            return consumers;
        }

        List<ConsumerTask> getUnassignedTasks() {
            return unassignedTasks;
        }

        WorkBalancingResult toWorkBalancingResult() {
            Map<SubscriptionName, Set<SubscriptionAssignment>> targetView = new HashMap<>();
            for (ConsumerNode consumer : consumers) {
                for (ConsumerTask consumerTask : consumer.getAssignedTasks()) {
                    SubscriptionName subscriptionName = consumerTask.getSubscriptionName();
                    Set<SubscriptionAssignment> assignments = targetView.computeIfAbsent(subscriptionName, ignore -> new HashSet<>());
                    assignments.add(new SubscriptionAssignment(consumer.getConsumerId(), subscriptionName));
                }
            }
            return new WorkBalancingResult(new SubscriptionAssignmentView(targetView), unassignedTasks.size());
        }
    }

    private static class MoveOutProposal {

        private final ConsumerNode overloaded;
        private final ConsumerNode underloaded;
        private final Weight finalOverloadedWeight;
        private final Weight finalUnderloadedWeight;

        MoveOutProposal(ConsumerNode overloaded, ConsumerNode underloaded, ConsumerTask taskFromOverloaded) {
            this.overloaded = overloaded;
            this.underloaded = underloaded;
            this.finalOverloadedWeight = overloaded.getWeight()
                    .subtract(taskFromOverloaded.getWeight());
            this.finalUnderloadedWeight = underloaded.getWeight()
                    .add(taskFromOverloaded.getWeight());
        }

        Weight getFinalUnderloadedWeight() {
            return finalUnderloadedWeight;
        }

        @Override
        public String toString() {
            return toString(overloaded, finalOverloadedWeight) + "\n" + toString(underloaded, finalUnderloadedWeight);
        }

        private String toString(ConsumerNode consumerNode, Weight newWeight) {
            return consumerNode + " (old weight = " + consumerNode.getWeight() + ", new weight = " + newWeight + ")";
        }
    }

    private static class SwapProposal {

        private final ConsumerNode overloaded;
        private final ConsumerNode underloaded;
        private final Weight finalOverloadedWeight;
        private final Weight finalUnderloadedWeight;

        SwapProposal(ConsumerNode overloaded,
                     ConsumerNode underloaded,
                     ConsumerTask taskFromOverloaded,
                     ConsumerTask taskFromUnderloaded) {
            this.overloaded = overloaded;
            this.underloaded = underloaded;
            this.finalOverloadedWeight = overloaded.getWeight()
                    .add(taskFromUnderloaded.getWeight())
                    .subtract(taskFromOverloaded.getWeight());
            this.finalUnderloadedWeight = underloaded.getWeight()
                    .add(taskFromOverloaded.getWeight())
                    .subtract(taskFromUnderloaded.getWeight());
        }

        Weight getOverloadedWeight() {
            return overloaded.getWeight();
        }

        Weight getFinalOverloadedWeight() {
            return finalOverloadedWeight;
        }

        Weight getFinalUnderloadedWeight() {
            return finalUnderloadedWeight;
        }

        @Override
        public String toString() {
            return toString(overloaded, finalOverloadedWeight) + "\n" + toString(underloaded, finalUnderloadedWeight);
        }

        private String toString(ConsumerNode consumerNode, Weight newWeight) {
            return consumerNode + " (old weight = " + consumerNode.getWeight() + ", new weight = " + newWeight + ")";
        }
    }

    private static class TargetConsumerLoad {

        private final Weight weight;
        private final int numberOfTasks;

        TargetConsumerLoad(Weight weight, int numberOfTasks) {
            this.weight = weight;
            this.numberOfTasks = numberOfTasks;
        }

        Weight getWeight() {
            return weight;
        }

        int getNumberOfTasks() {
            return numberOfTasks;
        }
    }
}