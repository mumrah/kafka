package kafka.server;

import kafka.cluster.Partition;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.share.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.ShareSessionCache;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchParams;

import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import scala.Option;
import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.requests.FetchRequest.CONSUMER_REPLICA_ID;
import static org.apache.kafka.common.requests.FetchRequest.INVALID_LOG_START_OFFSET;

public class SharePartitionManager2 {

    private static final Logger log = LoggerFactory.getLogger(SharePartitionManager.class);

    /**
     * The partition cache map is used to store the SharePartition objects for each share group topic-partition.
     */
    private Map<SharePartitionKey, SharePartition2> partitionCacheMap;

    /**
     * The replica manager is used to fetch messages from the log.
     */

    /**
     * The time instance is used to get the current time.
     */
    private final Time time;

    /**
     * The share session cache stores the share sessions.
     */
    private final ShareSessionCache cache;

    /**
     * The record lock duration is the time in milliseconds that a record lock is held for.
     */
    private final int recordLockDurationMs;

    /**
     * The timer is used to schedule the records lock timeout.
     */
    private final Timer timer;

    /**
     * The max in flight messages is the maximum number of messages that can be in flight at any one time per share-partition.
     */
    private final int maxInFlightMessages;

    /**
     * The max delivery count is the maximum number of times a message can be delivered before it is considered to be archived.
     */
    private final int maxDeliveryCount;

    /**
     * The persister is used to persist the share partition state.
     */
    private final Persister persister;

    /**
     * Class with methods to record share group metrics.
     */
    private final SharePartitionManager.ShareGroupMetrics shareGroupMetrics;

    private final SharePartitionReader reader;

    private final ReplicaManagerShim replicaManagerShim;

    public SharePartitionManager2(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        int recordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        Persister persister,
        Metrics metrics
    ) {
        this.replicaManagerShim = new ShareFetchReplicaManager(replicaManager);
        this.time = time;
        this.cache = cache;
        this.recordLockDurationMs = recordLockDurationMs;
        this.timer = new SystemTimerReaper("share-group-lock-timeout-reaper",
                new SystemTimer("share-group-lock-timeout"));
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxInFlightMessages = maxInFlightMessages;
        this.persister = persister;
        this.shareGroupMetrics = new SharePartitionManager.ShareGroupMetrics(Objects.requireNonNull(metrics), time);

        this.partitionCacheMap = new HashMap<>();
        this.reader = new SharePartitionReader(replicaManager, this);
    }


    Optional<SharePartition2> getSharePartition(
        TopicIdPartition topicIdPartition,
        String groupId
    ) {
        SharePartitionKey sharePartitionKey = new SharePartitionKey(
            groupId,
            topicIdPartition
        );
        return getSharePartition(sharePartitionKey);
    }

    Optional<SharePartition2> getSharePartition(
        SharePartitionKey key
    ) {
        return Optional.ofNullable(partitionCacheMap.get(key));
    }

    SharePartition2 getOrLoadSharePartition(
        TopicIdPartition topicIdPartition,
        String groupId,
        String memberId
    ) {
        SharePartitionKey sharePartitionKey = new SharePartitionKey(
            groupId,
            topicIdPartition
        );
        return partitionCacheMap.computeIfAbsent(sharePartitionKey, __ -> {
            long start = time.hiResClockMs();
            SharePartition2 sp = new SharePartition2(time);
            this.shareGroupMetrics.partitionLoadTime(start);
            log.info("SharePartition {} loaded by member {}", sp, memberId);
            return sp;
        });
    }

    void acknowledge(
        String memberId,
        String groupId,
        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics
    ) {
        acknowledgeTopics.forEach((topicIdPartition, acks) -> {
            SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);
            Optional<SharePartition2> sharePartitionOpt = getSharePartition(key);
            if (sharePartitionOpt.isPresent()) {
                SharePartition2 sharePartition = sharePartitionOpt.get();
                acks.forEach(batch -> {
                    log.info("acknowledge: {}", batch);
                    if (batch.acknowledgeTypes().size() == 1) {
                        sharePartition.inFlightRecords().acknowledgeOffsets(batch.firstOffset(), batch.lastOffset());
                    } else {
                        // TODO implement this
                        log.error("not implemented");
                    }
                });
            }
        });
    }

    public static int tryAcquireOnePartition(
        TopicIdPartition topicIdPartition,
        SharePartition2 sharePartition,
        int maxBytesRemaining,
        Map<TopicIdPartition, Integer> partitionMaxBytes,
        Map<TopicIdPartition, List<InFlightRecords.AcquiredRecords>> acquiredRecords
    ) {
        log.info("tryAcquireOnePartition({}, {})", sharePartition, partitionMaxBytes);
        int accumulatedBytes = 0;

        // Check how many bytes can be added to the response
        int maxBytesRem = Math.min(partitionMaxBytes.get(topicIdPartition), maxBytesRemaining);
        log.info("max bytes: {}", maxBytesRem);
        if (maxBytesRem > 0) {
            // Just acquire one batch. We could get additional batches here if desired.
            int acquireMaxBytes = maxBytesRem;
            if (acquiredRecords.isEmpty()) {
                // Ignore max bytes if we haven't acquired anything. Ensure we get at least one batch.
                acquireMaxBytes = 0;
            }
            Optional<InFlightRecords.AcquiredRecords> batchOpt = sharePartition.inFlightRecords().acquireFirstAvailable(acquireMaxBytes);
            if (batchOpt.isPresent()) {
                log.info("Acquired {}", batchOpt.get());
                int batchSize = batchOpt.get().sizeInBytes();
                partitionMaxBytes.computeIfPresent(topicIdPartition, (__, size) -> size - batchSize);
                accumulatedBytes += batchSize;
                acquiredRecords.compute(topicIdPartition, (__, batches) -> {
                    if (batches == null) {
                        batches = new ArrayList<>();
                    }
                    batches.add(batchOpt.get());
                    return batches;
                });
            } else {
                log.info("Nothing Acquired");
            }
        }

        return accumulatedBytes;
    }

    public static void tryAppendNewRecords(
        TopicIdPartition topicIdPartition,
        SharePartition2 sharePartition,
        ReplicaManagerShim replicaManagerShim
    ) {
        // With the read lock held, check if we have capacity in the queue and if there is new data in the log
        boolean shouldRead = sharePartition.inFlightRecords().inReadLock(() -> {
        if (sharePartition.inFlightRecords().totalRecords() > 15000) { // TODO configure this
            log.debug("In-flight queue is full, cannot append any more");
            return false;
        }

        LogOffsetMetadata usableEndOffset = replicaManagerShim.getUsableOffset(topicIdPartition, FetchIsolation.HIGH_WATERMARK); // TODO support last stable offset as well
        long fetchOffset = sharePartition.getNextFetchOffset().orElse(0L);

        // Only bother reading if the HWM is at least our next fetch offset
        return usableEndOffset.messageOffset >= fetchOffset;
        });

        if (!shouldRead) {
            return;
        }

        sharePartition.inFlightRecords().inWriteLock(() -> {
            // It's possible the queue changed since we had the read lock. Check again.
            if (sharePartition.inFlightRecords().totalRecords() > 15000) { // TODO configure this
                log.debug("In-flight queue is full, cannot append any more");
                return;
            }

            long fetchOffset = sharePartition.getNextFetchOffset().orElse(0L);

            // Read from RM
            FetchPartitionData data = replicaManagerShim.readOnePartition(topicIdPartition, fetchOffset, 1000);

            // Append to queue
            data.records.batches().forEach(batch ->
                sharePartition.inFlightRecords().appendAvailableRecords(batch.baseOffset(), batch.lastOffset(), batch.sizeInBytes()));
        });

    }

    public void fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions,
        Map<TopicIdPartition, Integer> partitionMaxBytes,
        Consumer<Map<TopicIdPartition, PartitionData>> responseCallback
    ) {
        log.info("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
                topicIdPartitions, groupId, fetchParams);

        // 1. If there is capacity in the queue, we should try to fill with new records. This helps bootstrap
        //    the queue as well as keep it full for future requests.
        Map<TopicIdPartition, SharePartition2> sharePartitions = new HashMap<>();
        for (TopicIdPartition topicIdPartition : topicIdPartitions) {
            SharePartition2 sharePartition = getOrLoadSharePartition(topicIdPartition, groupId, memberId);
            sharePartitions.put(topicIdPartition, sharePartition);
            tryAppendNewRecords(topicIdPartition, sharePartition, replicaManagerShim);
        }

        // 2. Next, read what is available from the in-flight queue. There is a race between this and the
        //    queue re-fill in 1. It's possible that another request will steal the newly appended records
        //    before we obtain the lock. Consider doing append and acquire in one block for each partition.
        int accumulatedBytes = 0;
        Map<TopicIdPartition, List<InFlightRecords.AcquiredRecords>> acquiredBatches = new HashMap<>();
        for (TopicIdPartition topicIdPartition : topicIdPartitions) {
            int acquiredBytes = tryAcquireOnePartition(
                topicIdPartition,
                sharePartitions.get(topicIdPartition),
                fetchParams.maxBytes - accumulatedBytes,
                partitionMaxBytes,
                acquiredBatches
            );
            accumulatedBytes += acquiredBytes;
            if (accumulatedBytes >= fetchParams.maxBytes) {
                break;
            }
        }

        // 3. If it's enough data to satisfy fetch params, we respond immediately.
        //    TODO can we cache FileRecords in the in-flight queue and avoid this read?
        if (accumulatedBytes >= fetchParams.minBytes || fetchParams.maxWaitMs <= 0) {
            ReplicaManagerShim.ReadManyBuilder builder = replicaManagerShim.readManyPartitions();
            acquiredBatches.forEach((topicIdPartition, batches) -> {
                SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);
                Optional<SharePartition2> sharePartitionOpt = getSharePartition(key);
                if (sharePartitionOpt.isPresent()) {
                    SharePartition2 sharePartition = sharePartitionOpt.get();
                    if (!batches.isEmpty()) {
                        long fetchOffset = batches.get(0).startOffset();
                        long lastOffset = batches.get(batches.size() - 1).startOffset();
                        int fetchSize = sharePartition.recordSize(fetchOffset, lastOffset);
                        builder.addPartition(topicIdPartition, fetchOffset, fetchSize);
                    }
                } else {
                    log.error("Missing share partition {}", key);
                }
            });
            Map<TopicIdPartition, FetchPartitionData> readResults = builder.build();
            Map<TopicIdPartition, PartitionData> responseData = new HashMap<>(readResults.size());
            for (TopicIdPartition topicIdPartition : topicIdPartitions) {
                FetchPartitionData partitionRead = readResults.get(topicIdPartition);
                List<InFlightRecords.AcquiredRecords> acquiredRecords = acquiredBatches.get(topicIdPartition);
                List<ShareFetchResponseData.AcquiredRecords> rpcAcquiredRecords = new ArrayList<>(acquiredRecords.size());
                log.info("Acquired records {}", acquiredRecords);
                acquiredRecords.forEach(records -> {
                    rpcAcquiredRecords.add(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(records.startOffset())
                        .setLastOffset(records.endOffset())
                        .setDeliveryCount((short) 1));
                });
                responseData.put(topicIdPartition, new PartitionData()
                    .setAcquiredRecords(rpcAcquiredRecords)
                    .setRecords(partitionRead.records)
                    .setErrorCode(partitionRead.error.code())
                    .setPartitionIndex(topicIdPartition.partition()));
            }

            responseCallback.accept(responseData);
            return;
        }

        // 4. There is not enough data to satisfy the fetch params, we create a delayed request with one or two
        //    watch keys. The first watch key is (topic, partition, group). This key is used to complete operations
        //    as records are acknowledged or timed out. The second watch is (topic, partition) is only used if the
        //    queue was not at capacity. It will be called from producer requests after appending to the log.

        DelayedShareFetch shareFetch = new DelayedShareFetch(
            groupId,
            memberId,
            fetchParams,
            topicIdPartitions,
            partitionMaxBytes,
            acquiredBatches,
            this,
            replicaManagerShim,
            responseCallback
        );
        log.info("Creating delayed share fetch");

        // Use (topic, partition, group) and (topic, partition) as watch keys
        Set<DelayedOperationKey> watchKeys = new HashSet<>(topicIdPartitions.size() * 2);
        topicIdPartitions.forEach(topicIdPartition -> {
            watchKeys.add(new SharePartitionOperationKey(topicIdPartition, groupId));
            watchKeys.add(new TopicPartitionOperationKey(topicIdPartition.topic(), topicIdPartition.partition()));
        });

        // This tryComplete can race with other request threads adding delayed share fetches.
        replicaManagerShim.addDelayedShareFetch(shareFetch, watchKeys);
    }
}
