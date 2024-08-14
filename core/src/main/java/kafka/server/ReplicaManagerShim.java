package kafka.server;


import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ReplicaManagerShim {

    interface ReadManyBuilder {
        void addPartition(
            TopicIdPartition topicIdPartition,
            long fetchOffset,
            int maxFetchSize
        );

        Map<TopicIdPartition, FetchPartitionData> build();
    }

    FetchPartitionData readOnePartition(
        TopicIdPartition topicIdPartition,
        long fetchOffset,
        int maxFetchSize
    );

    ReadManyBuilder readManyPartitions();

    boolean addDelayedShareFetch(DelayedShareFetch delayedShareFetch, Collection<DelayedOperationKey> watchKeys);

    void checkDelayedShareFetch(SharePartitionOperationKey key);

    LogOffsetMetadata getUsableOffset(TopicIdPartition topicIdPartition, FetchIsolation fetchIsolation);
}
