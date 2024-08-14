package kafka.server;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import static kafka.server.SharePartitionManager2.tryAcquireOnePartition;
import static kafka.server.SharePartitionManager2.tryAppendNewRecords;

public class DelayedShareFetch extends DelayedOperation {

    private final Logger log;
    private final String groupId;
    private final String memberId;
    private final FetchParams fetchParams;
    private final List<TopicIdPartition> topicIdPartitions;
    private final Map<TopicIdPartition, Integer> partitionMaxBytes;
    private final Map<TopicIdPartition, List<InFlightRecords.AcquiredRecords>> acquiredBatches;

    private final SharePartitionManager2 sharePartitionManager;
    private final ReplicaManagerShim replicaManagerShim;
    private final Consumer<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> responseCallback;
    private int accumulatedBytes;

    DelayedShareFetch(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions,
        Map<TopicIdPartition, Integer> partitionMaxBytes,
        Map<TopicIdPartition, List<InFlightRecords.AcquiredRecords>> acquiredBatches,
        SharePartitionManager2 sharePartitionManager,
        ReplicaManagerShim replicaManagerShim,
        Consumer<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> responseCallback
    ) {
        super(fetchParams.maxWaitMs, Option.empty());
        LogContext logContext = new LogContext(String.format("DelayedShareFetch(group=%s, member=%s): ", groupId, memberId));
        this.log = logContext.logger(DelayedShareFetch.class);
        this.log.info("DelayedShareFetch(maxWait={})", fetchParams.maxWaitMs);
        this.groupId = groupId;
        this.memberId = memberId;
        this.fetchParams = fetchParams;
        this.topicIdPartitions = topicIdPartitions;
        this.partitionMaxBytes = partitionMaxBytes;
        this.acquiredBatches = acquiredBatches;
        this.sharePartitionManager = sharePartitionManager;
        this.replicaManagerShim = replicaManagerShim;
        this.responseCallback = responseCallback;
        this.accumulatedBytes = 0;
    }

    @Override
    public void onExpiration() {
        log.info("onExpiration");
    }

    @Override
    public void onComplete() {
        log.info("onComplete");
        ReplicaManagerShim.ReadManyBuilder builder = replicaManagerShim.readManyPartitions();
        acquiredBatches.forEach((topicIdPartition, batches) -> {
            SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);
            Optional<SharePartition2> sharePartitionOpt = sharePartitionManager.getSharePartition(key);
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
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new HashMap<>(readResults.size());
        for (TopicIdPartition topicIdPartition : topicIdPartitions) {
            FetchPartitionData partitionRead = readResults.get(topicIdPartition);
            if (partitionRead == null) {
                log.info("Nothing read for {}", topicIdPartition);
                continue;
            }
            List<InFlightRecords.AcquiredRecords> acquiredRecords = acquiredBatches.get(topicIdPartition);
            if (acquiredRecords == null) {
                log.info("No records acquired for {}", topicIdPartition);
                continue;
            }
            List<ShareFetchResponseData.AcquiredRecords> rpcAcquiredRecords = new ArrayList<>(acquiredRecords.size());
            log.info("Acquired records {}", acquiredRecords);
            acquiredRecords.forEach(records -> {
                rpcAcquiredRecords.add(new ShareFetchResponseData.AcquiredRecords()
                    .setFirstOffset(records.startOffset())
                    .setLastOffset(records.endOffset())
                    .setDeliveryCount((short) 1));
            });
            responseData.put(topicIdPartition, new ShareFetchResponseData.PartitionData()
                    .setAcquiredRecords(rpcAcquiredRecords)
                    .setRecords(partitionRead.records)
                    .setErrorCode(partitionRead.error.code())
                    .setPartitionIndex(topicIdPartition.partition()));
        }

        log.info("deferred response {}", responseData);
        responseCallback.accept(responseData);
    }

    /**
     * Try to accumulate enough data to respond.
     * This will be called from a few places
     * 1. (topic, partition) -> Producer request handlers threads, after appending records
     * 2. (topic, partition, group) -> Acquisition timers expiring
     * 3. (topic, partition, group) -> Acknowledgments increasing capacity of the queue
     *
     */
    @Override
    public boolean tryComplete() {
        log.info("tryComplete");
        // This where we can strategize how to consume from the queue. Does each DelayedShareFetch
        // consume as much as possible in order to complete itself? Or does it only take one batch
        // at a time off the queue.

        for (TopicIdPartition topicIdPartition : topicIdPartitions) {
            // 1. Since this is our chance to do work on producer threads following an append, check to see
            //    if the queue has capacity for new records. If so, and new records are in the log, append them
            //    to the in-flight queue.
            SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);
            SharePartition2 sharePartition = sharePartitionManager.getSharePartition(key).orElseThrow(RuntimeException::new);
            tryAppendNewRecords(topicIdPartition, sharePartition, replicaManagerShim);

            // 2. Even if we did not read new data into the in-flight queue, we need to check for newly available
            //    records (via explicit release or acquisition timeout)
            int acquiredBytes = tryAcquireOnePartition(
                topicIdPartition,
                sharePartition,
                fetchParams.maxBytes - accumulatedBytes,
                partitionMaxBytes,
                acquiredBatches
            );
            accumulatedBytes += acquiredBytes;
        }

        if (accumulatedBytes >= fetchParams.minBytes) {
            log.info("Accumulated {} which is more than minBytes {}", accumulatedBytes, fetchParams.minBytes);
            return forceComplete();
        } else {
            log.info("Accumulated {} so far. Need at least {} to complete", accumulatedBytes, fetchParams.minBytes);
            return false;
        }
    }
}
