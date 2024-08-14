package kafka.server;

import org.apache.kafka.common.TopicIdPartition;

import java.util.Objects;

public class SharePartitionKey {
    private final String groupId;
    private final TopicIdPartition topicIdPartition;

    SharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        this.groupId = Objects.requireNonNull(groupId);
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition);
    }

    public String groupId() {
        return groupId;
    }

    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicIdPartition);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        else if (obj == null || getClass() != obj.getClass())
            return false;
        else {
            SharePartitionKey that = (SharePartitionKey) obj;
            return groupId.equals(that.groupId) && Objects.equals(topicIdPartition, that.topicIdPartition);
        }
    }

    @Override
    public String toString() {
        return "SharePartitionKey{" +
                "groupId='" + groupId +
                ", topicIdPartition=" + topicIdPartition +
                '}';
    }
}
