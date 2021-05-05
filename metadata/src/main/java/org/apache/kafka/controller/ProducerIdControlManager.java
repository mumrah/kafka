package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.ProducerIdRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Collections;

import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;


public class ProducerIdControlManager {
    private static final Object PRODUCER_ID_KEY = new Object();
    private static final int PRODUCER_ID_BLOCK_SIZE = 1000;

    final TimelineHashMap<Object, Long> lastProducerId;

    ProducerIdControlManager(SnapshotRegistry snapshotRegistry) {
        this.lastProducerId = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    ControllerResult<ResultOrError<ProducerIdRange>> generateNextProducerId(int brokerId, long brokerEpoch) {
        long producerId = lastProducerId.getOrDefault(PRODUCER_ID_KEY, 0L);

        if (producerId > Long.MAX_VALUE - PRODUCER_ID_BLOCK_SIZE) {
            ApiError error = new ApiError(UNKNOWN_SERVER_ERROR,
                "Exhausted all producerIds as the next block's end producerId " +
                "is will has exceeded long type limit");
            return ControllerResult.of(Collections.emptyList(), ResultOrError.of(error));
        }


        long nextProducerId = producerId + PRODUCER_ID_BLOCK_SIZE;

        ProducerIdRecord record = new ProducerIdRecord()
            .setProducerIdEnd(nextProducerId)
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);
        ProducerIdRange range = new ProducerIdRange(producerId, PRODUCER_ID_BLOCK_SIZE);
        return ControllerResult.of(
            Collections.singletonList(new ApiMessageAndVersion(record, (short) 0)), ResultOrError.of(range));
    }

    void replay(ProducerIdRecord record) {
        long currentProducerId = lastProducerId.getOrDefault(PRODUCER_ID_KEY, 0L);
        if (record.producerIdEnd() <= currentProducerId) {
            throw new RuntimeException("Producer ID from record is not monotonically increasing");
        } else {
            lastProducerId.put(PRODUCER_ID_KEY, record.producerIdEnd());
        }
    }

    static class ProducerIdRange {
        private final long producerIdStart;
        private final int producerIdLen;

        ProducerIdRange(long producerIdStart, int producerIdLen) {
            this.producerIdStart = producerIdStart;
            this.producerIdLen = producerIdLen;
        }

        public long producerIdStart() {
            return producerIdStart;
        }

        public int producerIdLen() {
            return producerIdLen;
        }
    }
}
