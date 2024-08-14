package kafka.server;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.kafka.common.requests.FetchRequest.CONSUMER_REPLICA_ID;
import static org.apache.kafka.common.requests.FetchRequest.INVALID_LOG_START_OFFSET;

public class SharePartitionReader {

    private final List<SharePartitionReaderThread> readerThreads;

    public SharePartitionReader(
        ReplicaManager replicaManager,
        SharePartitionManager2 sharePartitionManager
    ) {
        readerThreads = new ArrayList<>();
        readerThreads.add(new SharePartitionReaderThread("share-partition-reader-0", sharePartitionManager, replicaManager));
    }

    SharePartitionReaderThread findThread(String groupId, TopicIdPartition topicIdPartition) {
        SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);
        int idx = key.hashCode() % readerThreads.size();
        return readerThreads.get(idx);
    }

    public void maybeEnqueueRead(
        String groupId,
        TopicIdPartition topicIdPartition,
        long maxWaitMs,
        int maxPartitionBytes
    ) {
        findThread(groupId, topicIdPartition).maybeEnqueueRead(
            groupId, topicIdPartition, maxWaitMs, maxPartitionBytes);
    }


    enum SharePartitionReadState {
        NONE,
        PENDING,
        INFLIGHT,
        IDLE;
    }

    static class SharePartitionState {
        SharePartitionReadState readState;

        int maxPartitionBytes;

        long maxWaitMs;

        SharePartitionState(int maxPartitionBytes, long maxWaitMs) {
            this.readState = SharePartitionReadState.PENDING;
            this.maxPartitionBytes = maxPartitionBytes;
            this.maxWaitMs = maxWaitMs;
        }

        void maybeSetPending(int maxPartitionBytes, long maxWaitMs) {
            if (readState != SharePartitionReadState.INFLIGHT) {
                if (maxPartitionBytes < this.maxPartitionBytes) {
                    this.maxPartitionBytes = maxPartitionBytes;
                }
                if (maxWaitMs < this.maxWaitMs) {
                    this.maxWaitMs = maxWaitMs;
                }
                readState = SharePartitionReadState.PENDING;
            }
        }

        void setInFlight() {
            readState = SharePartitionReadState.INFLIGHT;
            maxPartitionBytes = Integer.MAX_VALUE;
            maxWaitMs = Integer.MAX_VALUE;
        }
    }

    static class SharePartitionInterest {
        final SharePartitionKey key;
        final int maxPartitionBytes;
        final long maxWaitMs;

        public SharePartitionInterest(SharePartitionKey key, int maxPartitionBytes, long maxWaitMs) {
            this.key = key;
            this.maxPartitionBytes = maxPartitionBytes;
            this.maxWaitMs = maxWaitMs;
        }
    }

    static class SharePartitionReaderThread extends ShutdownableThread {

        private final SharePartitionManager2 sharePartitionManager;

        private final ReplicaManager replicaManager;

        ReentrantLock lock;

        Condition pendingReadCond;

        Map<SharePartitionKey, SharePartitionState> sharePartitionStates;

        List<SharePartitionInterest> interests;

        public SharePartitionReaderThread(
            String name,
            SharePartitionManager2 sharePartitionManager,
            ReplicaManager replicaManager
        ) {
            super(name, false);
            this.sharePartitionManager = sharePartitionManager;
            this.replicaManager = replicaManager;
            this.lock = new ReentrantLock();
            this.pendingReadCond = lock.newCondition();
            this.sharePartitionStates = new ConcurrentHashMap<>();
            this.interests = new ArrayList<>();
        }

        /**
         * Try to enqueue a new share partition read. This will be called by the request threads handling
         * ShareFetch as well as the callback for ReplicaManager#fetchMessages.
         * @return true if the share partition read was enqueued or was already enqueued.
         *         false if the read was already in-flight.
         */
        public boolean maybeEnqueueRead(
            String groupId,
            TopicIdPartition topicIdPartition,
            long maxWaitMs,
            int maxPartitionBytes
        ) {
            SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);
            lock.lock();
            try {
                SharePartitionState newState = sharePartitionStates.compute(key, (__, partitionState) -> {
                    if (partitionState == null) {
                        return new SharePartitionState(maxPartitionBytes, maxWaitMs);
                    } else {
                        partitionState.maybeSetPending(maxPartitionBytes, maxWaitMs);
                        return partitionState;
                    }
                });
                pendingReadCond.signalAll();
                return newState.readState.equals(SharePartitionReadState.PENDING);
            } finally {
                lock.unlock();
            }
        }

        void processFetchMessages(String groupId, TopicIdPartition topicIdPartition, FetchPartitionData fetchPartitionData) {

            SharePartitionKey key = new SharePartitionKey(groupId, topicIdPartition);

            // 1. Indicate the fetch is done for this SharePartition
            sharePartitionStates.computeIfPresent(key, (__, partitionState) -> {
                if (partitionState.readState == SharePartitionReadState.INFLIGHT) {
                    partitionState.readState = SharePartitionReadState.IDLE;
                }
                return partitionState;
            });

            // 2. Append these messages as Available to the in-flight queue
            // We can either add each batch to the queue, or add the whole response as one large batch.
            Optional<SharePartition2> sharePartitionOpt = sharePartitionManager.getSharePartition(key);
            if (sharePartitionOpt.isPresent()) {
                fetchPartitionData.records.batches().forEach(batch -> {
                    sharePartitionOpt.get().appendAvailableRecords(batch.baseOffset(), batch.lastOffset(), batch.sizeInBytes());
                });
            }

            // 3. Notify waiting ShareFetch requests of new records
            //sharePartitionManager.tryCompleteShareFetches(key);
        }


        void tryNextRead() throws InterruptedException {
            lock.lock();
            try {
                interests.clear();
                sharePartitionStates.forEach((sharePartition, state) -> {
                    if (state.readState.equals(SharePartitionReadState.PENDING)) {
                        interests.add(new SharePartitionInterest(sharePartition, state.maxPartitionBytes, state.maxWaitMs));
                    }
                });

                if (interests.isEmpty()) {
                    log.debug("There are no pending SharePartition reads. Backing off for 1000ms before trying again");
                    pendingReadCond.await(1000, TimeUnit.MILLISECONDS);
                }
            } finally {
                lock.unlock();
            }

            interests.forEach(interest -> {
                // If no next offset, we cannot fetch
                Optional<Long> nextFetchOffset = sharePartitionManager.getSharePartition(interest.key)
                    .flatMap(SharePartition2::getNextFetchOffset);
                if (nextFetchOffset.isPresent()) {
                    Tuple2<TopicIdPartition, FetchRequest.PartitionData> fetch =
                        new Tuple2<>(interest.key.topicIdPartition(), new FetchRequest.PartitionData(
                            interest.key.topicIdPartition().topicId(),
                            nextFetchOffset.get(),
                            INVALID_LOG_START_OFFSET,  // Only used by replication
                            interest.maxPartitionBytes,
                            Optional.empty()
                        ));

                    FetchParams fetchParams = new FetchParams(
                        FetchRequestData.HIGHEST_SUPPORTED_VERSION,
                        CONSUMER_REPLICA_ID,
                        -1,
                        interest.maxWaitMs,
                        1,  // TODO how to decide this?
                        interest.maxPartitionBytes,
                        FetchIsolation.HIGH_WATERMARK,
                        Optional.empty()
                    );



                    sharePartitionStates.computeIfPresent(interest.key, (__, partitionState) -> {
                        partitionState.setInFlight();
                        return partitionState;
                    });
                    replicaManager.fetchMessages(
                        fetchParams,
                        CollectionConverters.asScala(Collections.singletonList(fetch)),
                        QuotaFactory.UnboundedQuota$.MODULE$,
                        responsePartitionData -> {
                            CollectionConverters.asJava(responsePartitionData).forEach(tuple ->
                                processFetchMessages(interest.key.groupId(), tuple._1, tuple._2)
                            );
                            return BoxedUnit.UNIT;
                        });
                } else {
                    log.error("Could not get next fetch offset for {}.", interest.key);
                }
            });
        }

        @Override
        public void doWork() {
            try {
                tryNextRead();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread was interrupted.", e);
            }
        }
    }
}
