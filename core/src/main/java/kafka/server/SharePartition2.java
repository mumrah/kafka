package kafka.server;

import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SharePartition2 {

    private final Time time;

    InFlightRecords records;

    SharePartition2(
        Time time
    ) {
        this.time = time;
        this.records = new InFlightRecords();
    }

    void appendAvailableRecords(long startOffset, long endOffset, int sizeInBytes) {
        records.appendAvailableRecords(startOffset, endOffset, sizeInBytes);
    }

    int recordSize(long startOffset, long endOffset) {
        return records.sliceSize(startOffset, endOffset);
    }

    Optional<Long> getNextFetchOffset() {
        return records.nextFetchOffset();
    }

    InFlightRecords inFlightRecords() {
        return records;
    }
}
