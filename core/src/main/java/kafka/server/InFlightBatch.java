package kafka.server;

import java.util.Arrays;

public class InFlightBatch implements NavigableBuffer.NavigableElement {
    private final long offset;

    private final short size;

    /**
     * Size in bytes as reported by LogReadInfo. This will be the transmitted size of this batch, not the size
     * of the acquired records.
     */
    private final int sizeInBytes;

    // TODO this will need to include the aborted transactions

    private boolean sparse;

    // These arrays are contiguous between (offset, offset + size) even if the underlying records are not
    // contiguous.
    private long[] acquiredNsArray;

    private short[] deliveryCountArray;

    private SharePartition.RecordState[] stateArray;

    public InFlightBatch(long startOffset, long endOffset, int sizeInBytes) {
        this.offset = startOffset;
        long size = endOffset - startOffset + 1;
        if (size > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot store a batch larger than " + Short.MAX_VALUE);
        }
        this.size = (short) size;
        this.sizeInBytes = sizeInBytes;
        this.sparse = true;
        this.acquiredNsArray = new long[] {0};
        this.deliveryCountArray = new short[] {0};
        this.stateArray = new SharePartition.RecordState[] {SharePartition.RecordState.AVAILABLE};
    }

    @Override
    public long startOffset() {
        return this.offset;
    }

    public long endOffset() {
        return this.offset + this.size - 1;
    }

    public int sizeInBytes() {
        return this.sizeInBytes;
    }

    /**
     * Check if this batch contains any AVAILABLE records. If so, transition them to ACQUIRED.
     * @return the number of acquired records, if any.
     */
    public int tryAcquire(int maxDeliveryCount) {
        if (sparse && stateArray[0] == SharePartition.RecordState.AVAILABLE) {
            stateArray[0] = SharePartition.RecordState.ACQUIRED;
            deliveryCountArray[0] += 1;
            return this.size;
        } else if (!sparse) {
            int acquired = 0;
            for (int i = 0; i < stateArray.length; i++) {
                if (stateArray[i] == SharePartition.RecordState.AVAILABLE && deliveryCountArray[i] < maxDeliveryCount) {
                    stateArray[i] = SharePartition.RecordState.ACQUIRED;
                    deliveryCountArray[i] += 1;
                    acquired += 1;
                }
            }
            return acquired;
        } else {
            return 0;
        }
    }

    public boolean isAcknowledged() {
        if (sparse) {
            return stateArray[0] == SharePartition.RecordState.ACKNOWLEDGED;
        } else {
            for (SharePartition.RecordState recordState : stateArray) {
                if (recordState != SharePartition.RecordState.ACKNOWLEDGED) {
                    return false;
                }
            }
            return true;
        }
    }

    public int updateStateForRange(SharePartition.RecordState state, long releaseStartOffset, long releaseEndOffset) {
        if (releaseStartOffset <= this.startOffset() && releaseEndOffset >= this.endOffset()) {
            // If the released offsets completely span this batch, leave it sparse.
            if (sparse) {
                stateArray[0] = state;
            } else {
                Arrays.fill(stateArray, state);
            }
            return size;
        } else {
            maybeDensify();
            // Release offsets from A to B
            // [ 100 101 102 103 104 ]
            //        A       B
            int startIdx = (int) Math.max(0, (releaseStartOffset - startOffset()));
            int endIdx = (int) Math.min(size, (releaseEndOffset - startOffset()));
            Arrays.fill(stateArray, startIdx, endIdx + 1, state);
            return (endIdx - startIdx) + 1;
        }
    }

    public int updateStateForOffsets(
        SharePartition.RecordState state,
        long[] offsets
    ) {
        maybeDensify();
        // Set state by offset
        // [ 100 101 102 103 104 ]
        //        x   x       x
        int updated = 0;
        for (long offset : offsets) {
            if (offset < startOffset()) {
                continue;
            }
            if (offset > endOffset()) {
                break;
            }
            int idx = (int) (offset - startOffset());
            stateArray[idx] = state;
            updated += 1;
        }
        return updated;
    }

    private void maybeDensify() {
        if (sparse) {
            long acquiredNs = acquiredNsArray[0];
            short deliveryCount = deliveryCountArray[0];
            SharePartition.RecordState state = stateArray[0];
            acquiredNsArray = new long[size];
            deliveryCountArray = new short[size];
            stateArray = new SharePartition.RecordState[size];
            Arrays.fill(acquiredNsArray, acquiredNs);
            Arrays.fill(deliveryCountArray, deliveryCount);
            Arrays.fill(stateArray, state);
            sparse = false;
        }
    }

    @Override
    public String toString() {
        return "InFlightBatch{" +
            "startOffset=" + startOffset() +
            ", endOffset=" + endOffset() +
            ", sizeInBytes=" + sizeInBytes() +
            ", state=" + Arrays.toString(stateArray) +
            ", deliveryCount=" + Arrays.toString(deliveryCountArray) +
            ", acquiredNs=" + Arrays.toString(acquiredNsArray) +
            '}';
    }

}
