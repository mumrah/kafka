package kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class InFlightRecords {

    private final Logger log = LoggerFactory.getLogger("InFlightRecords");

    @Override
    public String toString() {
        return "InFlightRecords{" +
                "availableOffset=" + availableOffset +
                ", nextFetchOffset=" + nextFetchOffset +
                ", availableOffsetCount=" + availableOffsetCount +
                ", buffer=" + buffer +
                '}';
    }

    public static class AcquiredRecords {
        private final InFlightBatch batch;

        AcquiredRecords(InFlightBatch batch) {
            this.batch = batch;
        }

        int sizeInBytes() {
            return batch.sizeInBytes();
        }

        long startOffset() {
            return batch.startOffset();
        }

        long endOffset() {
            return batch.endOffset();
        }

        @Override
        public String toString() {
            return "AcquiredRecords{" +
                    "sizeInBytes=" + batch.sizeInBytes() +
                    ", startOffset=" + batch.startOffset() +
                    ", endOffset=" + batch.endOffset() +
                    '}';
        }
    }

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final NavigableBuffer<InFlightBatch> buffer = new NavigableBuffer<>(32);

    /**
     * Points to the start offset of the batch containing the next available offset for acquisition.
     */
    private long availableOffset = -1;

    /**
     * Points to the next offset we should fetch to add new available records to the queue.
     */
    private long nextFetchOffset = -1;

    /**
     * An approximate count of the number of available offsets. This does not account for offset gaps between batches.
     */
    private int availableOffsetCount = 0;

    public int availableRecords() {
        lock.readLock().lock();
        try {
            // TODO maybe lock isn't needed here. Is a stale count okay?
            log.info("availableRecords: {}", availableOffsetCount);
            return availableOffsetCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int totalRecords() {
        lock.readLock().lock();
        try {
            InFlightBatch oldest = buffer.peek();
            if (oldest == null) {
                return 0;
            } else {
                InFlightBatch newest = buffer.peekLast();
                log.info("totalRecords: {}", (newest.endOffset() - oldest.startOffset() + 1));
                return (int) (newest.endOffset() - oldest.startOffset() + 1);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void inReadLock(Runnable runnable) {
        lock.readLock().lock();
        try {
            runnable.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    public <T> T inReadLock(Supplier<T> supplier) {
        lock.readLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void inWriteLock(Runnable runnable) {
        lock.writeLock().lock();
        try {
            runnable.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void appendAvailableRecords(long startOffset, long endOffset, int sizeInBytes) {
        InFlightBatch batch = new InFlightBatch(startOffset, endOffset, sizeInBytes);
        lock.writeLock().lock();
        try {
            if (availableOffset == -1) {
                availableOffset = startOffset;
            }
            buffer.add(batch);
            availableOffsetCount += (endOffset - startOffset + 1);
            nextFetchOffset = endOffset + 1;
            log.info("appendAvailableRecords({}, {}, {}): nextFetchOffset: {}", startOffset, endOffset, sizeInBytes, nextFetchOffset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int sliceSize(long startOffset, long endOffset) {
        lock.readLock().lock();
        try {
            Iterator<InFlightBatch> it = buffer.iterator(startOffset, endOffset);
            int sizeInBytes = 0;
            while (it.hasNext()) {
                InFlightBatch batch = it.next();
                sizeInBytes += batch.sizeInBytes();
            }
            log.info("sliceSize({}, {}): {}", startOffset, endOffset, sizeInBytes);
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<AcquiredRecords> acquireFirstAvailable(int maxBytes) {
        lock.readLock().lock();
        try {
            if (buffer.size() == 0) {
                return Optional.empty();
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if (buffer.size() == 0) {
                return Optional.empty();
            }
            Iterator<InFlightBatch> it = buffer.iterator(availableOffset);
            // TODO don't need loop here probably
            while (it.hasNext()) {
                InFlightBatch batch = it.next();
                if (maxBytes > 0 && batch.sizeInBytes() > maxBytes) {
                    // TODO should we scan for a smaller batch here?
                    log.info("acquireFirstAvailable({}): Batch {} larger than maxBytes", maxBytes, batch);
                    break;
                }

                if (maxBytes == 0 && batch.sizeInBytes() > maxBytes) {
                    log.info("acquireFirstAvailable({}): Acquiring a batch with size {} which is larger than maxBytes", batch.sizeInBytes(), maxBytes);
                }
                int acquired = batch.tryAcquire(3);
                if (acquired > 0) {
                    // If we acquired a batch, move up the availableOffset
                    if (it.hasNext()) {
                        availableOffset = it.next().startOffset();
                    } else {
                        // No more batches to acquire.
                        availableOffset = -1;
                    }
                    availableOffsetCount -= acquired;
                    return Optional.of(new AcquiredRecords(batch));
                } else {
                    // Could not acquire anything from availableOffset, so just break
                    break;
                }
            }
            return Optional.empty();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void releaseOffsets(long startOffset, long endOffset) {
        lock.writeLock().lock();
        try {
            Iterator<InFlightBatch> it = buffer.iterator(startOffset, endOffset);
            long firstReleasedOffset = -1;
            while (it.hasNext()) {
                InFlightBatch batch = it.next();
                int releasedCount = batch.updateStateForRange(SharePartition.RecordState.AVAILABLE, startOffset, endOffset);
                if (releasedCount > 0) {
                    if (firstReleasedOffset == -1) {
                        firstReleasedOffset = startOffset;
                    }
                    availableOffsetCount += releasedCount;
                } else {
                    break;
                }
            }
            if (availableOffset == -1 || availableOffset > firstReleasedOffset) {
                availableOffset = firstReleasedOffset;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void acknowledgeOffsetsArray(long[] offsets) {
        lock.writeLock().lock();
        try {
            long startOffset = offsets[0];
            long endOffset = offsets[offsets.length - 1];
            Iterator<InFlightBatch> it = buffer.iterator(startOffset, endOffset);
            while (it.hasNext()) {
                InFlightBatch batch = it.next();
                log.info("acknowledgeOffsets({}, {})", startOffset, endOffset);
                int ackedCount = batch.updateStateForOffsets(SharePartition.RecordState.ACKNOWLEDGED, offsets);
                if (ackedCount == 0) {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void acknowledgeOffsets(long startOffset, long endOffset) {
        lock.writeLock().lock();
        try {
            Iterator<InFlightBatch> it = buffer.iterator(startOffset, endOffset);
            while (it.hasNext()) {
                InFlightBatch batch = it.next();
                log.info("acknowledgeOffsets({}, {})", startOffset, endOffset);
                int ackedCount = batch.updateStateForRange(SharePartition.RecordState.ACKNOWLEDGED, startOffset, endOffset);
                if (ackedCount == 0) {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void archiveOffsets() {
        lock.writeLock().lock();
        try {
            InFlightBatch batch = buffer.peek();
            while (batch.isAcknowledged()) {
                buffer.poll();
                batch = buffer.peek();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<Long> nextFetchOffset() {
        lock.readLock().lock();
        try {
            if (nextFetchOffset == -1) {
                log.info("nextFetchOffset(): none");
                return Optional.empty();
            } else {
                log.info("nextFetchOffset(): {}", nextFetchOffset);
                return Optional.of(nextFetchOffset);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public static void main(String[] args) {
        InFlightRecords queue = new InFlightRecords();
        for (int i=0; i<100; i++) {
            queue.appendAvailableRecords(10*i, 10*i + 9, 42);
        }
        //queue.appendAvailableRecords(100, 200, 42);
        //queue.appendAvailableRecords(201, 220, 10);
        //queue.appendAvailableRecords(221, 300, 50);
        System.err.println("Available " + queue.availableRecords());
        System.err.println("Total " + queue.totalRecords());
        System.err.println(queue.nextFetchOffset());

        System.err.println(queue.acquireFirstAvailable(100));
        System.err.println("Available " + queue.availableRecords());
        System.err.println("Total " + queue.totalRecords());

        queue.acknowledgeOffsets(2, 4);
        System.err.println("Available " + queue.availableRecords());
        System.err.println("Total " + queue.totalRecords());
        System.err.println(queue);
        queue.archiveOffsets();
        System.err.println("Available " + queue.availableRecords());
        System.err.println("Total " + queue.totalRecords());
        queue.acknowledgeOffsets(0, 9);
        System.err.println(queue);
        queue.archiveOffsets();
        System.err.println(queue);
        System.err.println("Available " + queue.availableRecords());
        System.err.println("Total " + queue.totalRecords());

        /*

        System.err.println(queue);

        System.err.println(queue.acquireFirstAvailable(100));
        System.err.println(queue.acquireFirstAvailable(100));
        System.err.println(queue.acquireFirstAvailable(100));
        System.err.println(queue.acquireFirstAvailable(100));

        System.err.println(queue);

        queue.releaseOffsets(100, 110);
        System.err.println(queue);

        System.err.println(queue.acquireFirstAvailable(100));

        System.err.println(queue);

        */
    }
}
