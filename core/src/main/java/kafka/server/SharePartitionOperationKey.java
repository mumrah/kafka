package kafka.server;

import org.apache.kafka.common.TopicIdPartition;

import java.util.Objects;

public class SharePartitionOperationKey implements DelayedOperationKey {
    private final TopicIdPartition topicIdPartition;

    private final String groupId;

    SharePartitionOperationKey(TopicIdPartition topicIdPartition, String groupId) {
        this.topicIdPartition = topicIdPartition;
        this.groupId = groupId;
    }

    @Override
    public String keyLabel() {
        return String.format("%s-%d-%s", topicIdPartition.topic(), topicIdPartition.partition(), groupId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SharePartitionOperationKey that = (SharePartitionOperationKey) o;
        return topicIdPartition.equals(that.topicIdPartition) && groupId.equals(that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicIdPartition, groupId);
    }
}

