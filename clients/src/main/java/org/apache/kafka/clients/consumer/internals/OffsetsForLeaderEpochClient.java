package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Convenience class for making asynchronous requests to the OffsetsForLeaderEpoch API
 */
public class OffsetsForLeaderEpochClient extends AsyncClient<
        Map<TopicPartition, SubscriptionState.FetchPosition>,
        OffsetsForLeaderEpochRequest,
        OffsetsForLeaderEpochResponse,
        OffsetsForLeaderEpochClient.OffsetForEpochResult> {

    OffsetsForLeaderEpochClient(ConsumerNetworkClient client) {
        super(client);
    }

    @Override
    protected AbstractRequest.Builder<OffsetsForLeaderEpochRequest> prepareRequest(
            Node node, Map<TopicPartition, SubscriptionState.FetchPosition> requestData) {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> partitionData = new HashMap<>(requestData.size());
        requestData.forEach(((topicPartition, fetchPosition) -> fetchPosition.lastFetchEpoch.ifPresent(
            fetchEpoch -> partitionData.put(topicPartition,
                new OffsetsForLeaderEpochRequest.PartitionData(fetchPosition.currentLeader.epoch, fetchEpoch)))));

        return new OffsetsForLeaderEpochRequest.Builder(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), partitionData);
    }

    @Override
    protected OffsetForEpochResult handleResponse(
            Node node,
            Map<TopicPartition, SubscriptionState.FetchPosition> requestData,
            OffsetsForLeaderEpochResponse response) {

        Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();
        Map<TopicPartition, EpochEndOffset> endOffsets = new HashMap<>();

        for (TopicPartition topicPartition : requestData.keySet()) {
            EpochEndOffset epochEndOffset = response.responses().get(topicPartition);
            if (epochEndOffset == null) {
                log.warn("Missing partition {} from response, ignoring", topicPartition);
                continue;
            }
            Errors error = epochEndOffset.error();
            if (error == Errors.NONE) {
                log.debug("Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}",
                        topicPartition, epochEndOffset.endOffset(), epochEndOffset.leaderEpoch());
                endOffsets.put(topicPartition, epochEndOffset);
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
                    error == Errors.REPLICA_NOT_AVAILABLE ||
                    error == Errors.KAFKA_STORAGE_ERROR ||
                    error == Errors.OFFSET_NOT_AVAILABLE ||
                    error == Errors.LEADER_NOT_AVAILABLE) {
                log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                        topicPartition, error);
                partitionsToRetry.add(topicPartition);
            } else if (error == Errors.FENCED_LEADER_EPOCH ||
                    error == Errors.UNKNOWN_LEADER_EPOCH) {
                log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                        topicPartition, error);
                partitionsToRetry.add(topicPartition);
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
                partitionsToRetry.add(topicPartition);
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                unauthorizedTopics.add(topicPartition.topic());
            } else {
                log.warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.", topicPartition, error.message());
                partitionsToRetry.add(topicPartition);
            }
        }

        if (!unauthorizedTopics.isEmpty())
            throw new TopicAuthorizationException(unauthorizedTopics);
        else
            return new OffsetForEpochResult(endOffsets, partitionsToRetry);
    }

    public static class OffsetForEpochResult {
        private final Map<TopicPartition, EpochEndOffset> endOffsets;
        private final Set<TopicPartition> partitionsToRetry;

        OffsetForEpochResult(Map<TopicPartition, EpochEndOffset> endOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.endOffsets = endOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        public Map<TopicPartition, EpochEndOffset> endOffsets() {
            return endOffsets;
        }

        public Set<TopicPartition> partitionsToRetry() {
            return partitionsToRetry;
        }
    }
}
