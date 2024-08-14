package kafka.server;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An unbounded buffer based on an array.
 * @param <T>
 */
public class NavigableBuffer<T extends NavigableBuffer.NavigableElement> {

    @Override
    public String toString() {
        return "NavigableBuffer{" +
                "head=" + head +
                ", tail=" + tail +
                ", buffer=" + Arrays.toString(buffer) +
                '}';
    }

    public interface NavigableElement {
        long startOffset();
    }

    private static class FloorElement implements NavigableElement {
        private final long offset;

        private FloorElement(long offset) {
            this.offset = offset;
        }

        @Override
        public long startOffset() {
            return offset;
        }
    }

    private static class NavigableElementComparator implements Comparator<NavigableElement> {
        @Override
        public int compare(NavigableElement o1, NavigableElement o2) {
            return Long.compare(o1.startOffset(), o2.startOffset());
        }
    }

    private NavigableElement[] buffer;

    private int head;

    private int tail;

    /**
     * Mapping of offset to array index
     */
    private final TreeMap<Long, Integer> offsetIndex;

    /**
     * Constructs an UnboundedFifoBuffer with the specified number of elements.
     * The integer must be a positive integer.
     *
     * @param initialSize  the initial size of the buffer
     * @throws IllegalArgumentException  if the size is less than 1
     */
    public NavigableBuffer(int initialSize) {
        if (initialSize <= 0) {
            throw new IllegalArgumentException("The size must be greater than 0");
        }
        buffer = new NavigableElement[initialSize + 1];
        head = 0;
        tail = 0;
        offsetIndex = new TreeMap<>();
    }

    public int size() {
        int size;

        if (tail < head) {
            size = buffer.length - head + tail;
        } else {
            size = tail - head;
        }

        return size;
    }

    public boolean isEmpty() {
        return (size() == 0);
    }

    @SuppressWarnings("unchecked")
    T find(long offset) {
        if (tail < head) {
            FloorElement searchElem = new FloorElement(offset);
            int headToEnd = Arrays.binarySearch(
                buffer,
                head,
                buffer.length,
                searchElem,
                new NavigableElementComparator()
            );

            if (headToEnd >= 0) {
                // Exact hit
                return (T) buffer[headToEnd];
            } else if (headToEnd < -1) {
                int prev = Math.abs(headToEnd + 1) - 1;
                if (prev == buffer.length - 1 && buffer[0].startOffset() > offset) {
                    // hit in the last entry
                    return (T) buffer[prev];
                }
                if (prev < buffer.length - 1) {
                    return (T) buffer[prev];
                }
            }

            int startToTail = Arrays.binarySearch(
                buffer,
                0,
                tail,
                searchElem,
                new NavigableElementComparator()
            );

            if (startToTail >= 0) {
                // Exact hit
                return (T) buffer[startToTail];
            } else if (startToTail == -1) {
                return null;
            } else {
                int prev = Math.abs(startToTail + 1) - 1;
                return (T) buffer[prev];
            }

        } else {
            int idx = Arrays.binarySearch(
                buffer,
                head,
                tail,
                new FloorElement(offset),
                new NavigableElementComparator()
            );
            if (idx >= 0) {
                // Exact hit
                return (T) buffer[idx];
            } else if (idx == -1) {
                // Nothing found, all entries are greater than the search offset
                return null;
            } else {
                // Returns the insertion point encoded as (-(insertion point) - 1). We want the previous element.
                int prev = Math.abs(idx + 1) - 1;
                return (T) buffer[prev];
            }
        }
    }

    @SuppressWarnings("unchecked")
    public boolean add(T entry) {
        if (entry == null) {
            throw new NullPointerException("Attempted to add null object to buffer");
        }

        // If adding a new element will cause the tail to crash into the head, we need to grow
        // the underlying array and rebuild the index.
        if (size() + 1 >= buffer.length) {
            offsetIndex.clear();
            NavigableElement[] newBuffer = new NavigableElement[((buffer.length - 1) * 2) + 1];
            int newIndex = 0;
            // move head to element zero in the new array
            for (int i = head; i != tail;) {
                T el = (T) buffer[i];
                newBuffer[newIndex] = el;
                offsetIndex.put(el.startOffset(), newIndex);
                buffer[i] = null;
                newIndex++;
                i = increment(i);
            }
            buffer = newBuffer;
            head = 0;
            tail = newIndex;
        }

        buffer[tail] = entry;
        offsetIndex.put(entry.startOffset(), tail);
        tail = increment(tail);
        return true;
    }

    @SuppressWarnings("unchecked")
    public T peek() {
        if (isEmpty()) {
            return null;
        }
        return (T) buffer[head];
    }

    @SuppressWarnings("unchecked")
    public T peekLast() {
        if (isEmpty()) {
            return null;
        }
        // tail points to next slot
        return (T) buffer[tail - 1];
    }

    @SuppressWarnings("unchecked")
    public T poll() {
        if (isEmpty()) {
            return null;
        }

        T element = (T) buffer[head];
        if (element != null) {
            buffer[head] = null;
            offsetIndex.remove(element.startOffset());
            head = increment(head);
        }
        return element;
    }

    private int increment(int index) {
        index++;
        if (index >= buffer.length) {
            index = 0;
        }
        return index;
    }

    /**
     * Return an iterator that starts at element containing the given start offset
     */
    public Iterator<T> iterator(long startOffset) {
        Map.Entry<Long, Integer> startEntry = offsetIndex.floorEntry(startOffset);
        final int startIndex;
        if (startEntry == null) {
            startIndex = head;
        } else {
            startIndex = startEntry.getValue();
        }

        return iterator(startIndex, tail);
    }

    /**
     * Return an iterator that starts at element containing the given start offset and
     * stops at the element containing the given end offset
     */
    public Iterator<T> iterator(long startOffset, long endOffset) {
        Map.Entry<Long, Integer> startEntry = offsetIndex.floorEntry(startOffset);
        Map.Entry<Long, Integer> endEntry = offsetIndex.floorEntry(endOffset);

        final int startIndex;
        if (startEntry == null) {
            startIndex = head;
        } else {
            startIndex = startEntry.getValue();
        }

        final int endIndex;
        if (endEntry == null) {
            endIndex = tail;
        } else {
            endIndex = increment(endEntry.getValue());
        }

        return iterator(startIndex, endIndex);
    }

    /**
     * Return an iterator for this entire buffer
     */
    public Iterator<T> iterator() {
        return iterator(head, tail);
    }

    @SuppressWarnings("unchecked")
    private Iterator<T> iterator(int startIndex, int endIndex) {
        return new Iterator<T>() {
            private int index = startIndex;


            public boolean hasNext() {
                return index != endIndex;
            }

            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                int current = index;
                index = increment(index);
                return (T) buffer[current];
            }
        };
    }

    public static void main(String[] args) throws Exception {
        NavigableBuffer<InFlightBatch> buffer = new NavigableBuffer<>(32);
        for (int i=0; i<20; i++) {
            InFlightBatch batch = new InFlightBatch(0, 0, 42);
            buffer.add(batch);
            buffer.poll();
        }

        for (int i=0; i<20; i++) {
            InFlightBatch batch = new InFlightBatch(10*i, 10*i + 9, 42);
            buffer.add(batch);
        }

        for (int i=0; i<buffer.buffer.length; i++) {
            NavigableElement el = buffer.buffer[i];
            if (el != null) {
                System.err.println(i + ": " + el.startOffset());
            } else {
                System.err.println(i + ": " + el);
            }
        }
        for (int i=0; i<200; i++) {
            System.err.println("find(" + i + "): " + buffer.find(i));
        }
    }
}
