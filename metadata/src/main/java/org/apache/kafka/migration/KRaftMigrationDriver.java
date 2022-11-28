/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.migration;

import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class orchestrates and manages the state related to a ZK to KRaft migration. An event thread is used to
 * serialize events coming from various threads and listeners.
 */
public class KRaftMigrationDriver {

    class MetadataLogListener implements KRaftMetadataListener {
        MetadataImage image = MetadataImage.EMPTY;
        MetadataDelta delta = new MetadataDelta(image);

        @Override
        public void handleLeaderChange(boolean isActive, int epoch) {
            eventQueue.append(new KRaftLeaderEvent(isActive, nodeId, epoch));
        }

        @Override
        public void handleRecord(long offset, int epoch, ApiMessage record) {
            if (record.apiKey() == MetadataRecordType.NO_OP_RECORD.id()) {
                return;
            }

            eventQueue.append(new EventQueue.Event() {
                @Override
                public void run() {
                    if (delta == null) {
                        delta = new MetadataDelta(image);
                    }
                    delta.replay(offset, epoch, record);
                    image = delta.apply();
                    eventQueue.append(new SendRPCsToBrokersEvent(Optional.of(delta)));
                    delta = new MetadataDelta(image);
                }

                @Override
                public void handleException(Throwable e) {
                    log.error("Had an exception in " + this.getClass().getSimpleName(), e);
                }
            });
        }

        public MetadataImage image() {
            return image;
        }

        public boolean zkBrokersReadyForMigration(Set<Integer> expectedBrokers) {
            Set<Integer> knownZkBrokers = image.cluster().zkBrokers().keySet();
            knownZkBrokers.retainAll(expectedBrokers);
            return knownZkBrokers.equals(expectedBrokers);
        }

        ZkWriteEvent syncMetadataToZkEvent() {
            return new ZkWriteEvent() {
                @Override
                public void run() throws Exception {
                    if (delta == null) {
                        return;
                    }

                    log.info("Writing metadata changes to ZK");
                    try {
                        apply("Sync to ZK", __ -> migrationState(delta.highestOffset(), delta.highestEpoch()));
                        if (delta.topicsDelta() != null) {
                            delta.topicsDelta().changedTopics().forEach((topicId, topicDelta) -> {
                                // Ensure the topic exists
                                if (image.topics().getTopic(topicId) == null) {
                                    apply("Create topic " + topicDelta.name(), migrationState -> client.createTopic(topicDelta.name(), topicId, topicDelta.partitionChanges(), migrationState));
                                } else {
                                    apply("Updating topic " + topicDelta.name(), migrationState -> client.updateTopicPartitions(Collections.singletonMap(topicDelta.name(), topicDelta.partitionChanges()), migrationState));
                                }
                            });
                        }
                    } finally {
                        image = delta.apply();
                        delta = null;
                    }
                }
            };
        }
    }

    class SendRPCsToBrokersEvent implements EventQueue.Event {
        private final MetadataDelta delta;

        SendRPCsToBrokersEvent(Optional<MetadataDelta> deltaOpt) {
            this.delta = deltaOpt.orElse(null);
        }

        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case DUAL_WRITE:
                    log.info("Generating RPCs to the brokers");
                    if (delta == null) {
                        client.sendRequestsForBrokersFromImage(recoveryState.kraftControllerEpoch());
                    } else {
                        client.sendRequestsForBrokersFromDelta(delta, recoveryState.kraftControllerEpoch());
                    }
                    break;
                default:
                    // In other migration states we don't send RPCs to the brokers yet as the
                    // KRaft controller is not ready to handle the zkBrokers (dual-write) mode.
                    break;
            }
        }

    }

    // TODO: Probably not required with periodic RPCs. Check and remove.
    abstract class RPCResponseEvent<T extends ApiMessage> implements EventQueue.Event {
        private final int brokerId;
        private final T data;

        RPCResponseEvent(int brokerId, T data) {
            this.brokerId = brokerId;
            this.data = data;
        }

        int brokerId() {
            return brokerId;
        }
        T data() {
            return data;
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    abstract class ZkWriteEvent implements EventQueue.Event {
        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class UpdateMetadataResponseEvent extends RPCResponseEvent<UpdateMetadataResponseData> {
        UpdateMetadataResponseEvent(int brokerId, UpdateMetadataResponseData data) {
            super(brokerId, data);
        }

        @Override
        public void run() throws Exception {
            // TODO handle UMR response
        }
    }

    class PollEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case UNINITIALIZED:
                    initializeMigrationState();
                    break;
                case INACTIVE:
                    // Nothing to do when the driver is inactive. We need to wait on the
                    // controller node's state to move forward.
                    break;
                case WAIT_FOR_CONTROLLER_QUORUM:
                    // TODO
                    break;
                case BECOME_CONTROLLER:
                    // This probably means we are retrying
                    eventQueue.append(new BecomeZkLeaderEvent());
                    break;
                case WAIT_FOR_BROKERS:
                    eventQueue.append(new WaitForBrokersEvent());
                    break;
                case ZK_MIGRATION:
                    eventQueue.append(new MigrateMetadataEvent());
                    break;
                case DUAL_WRITE:
                    eventQueue.append(listener.syncMetadataToZkEvent());
                    break;
            }

            // Poll again after some time
            long deadline = time.nanoseconds() + NANOSECONDS.convert(1, SECONDS);
            eventQueue.scheduleDeferred(
                "poll",
                new EventQueue.DeadlineFunction(deadline),
                new PollEvent());
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class MigrateMetadataEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            if (migrationState != MigrationState.ZK_MIGRATION) {
                log.warn("Skipping ZK migration, already done");
                return;
            }

            Set<Integer> brokersWithAssignments = new HashSet<>();
            log.info("Begin migration from ZK");
            consumer.beginMigration();
            try {
                // TODO use a KIP-868 metadata transaction here
                List<CompletableFuture<?>> futures = new ArrayList<>();
                client.readAllMetadata(batch -> futures.add(consumer.acceptBatch(batch)), brokersWithAssignments::add);
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).get();

                Set<Integer> brokersWithRegistrations = new HashSet<>(zkBrokerRegistrations.keySet());
                brokersWithAssignments.removeAll(brokersWithRegistrations);
                if (!brokersWithAssignments.isEmpty()) {
                    //throw new IllegalStateException("Cannot migrate data with offline brokers: " + brokersWithAssignments);
                    log.error("Offline ZK brokers detected: {}", brokersWithAssignments);
                }

                // Update the migration state
                OffsetAndEpoch offsetAndEpoch = consumer.completeMigration();
                apply("Migrating ZK to KRaft", __ -> migrationState(offsetAndEpoch.offset(), offsetAndEpoch.epoch()));
            } catch (Throwable t) {
                log.error("Migration failed", t);
                consumer.abortMigration();
            } finally {
                // TODO Just skip to dual write for now
                apply("Persist recovery state", client::setMigrationRecoveryState);
                transitionTo(MigrationState.DUAL_WRITE);
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class KRaftLeaderEvent implements EventQueue.Event {
        private final boolean isActive;
        private final int kraftControllerId;
        private final int kraftControllerEpoch;

        KRaftLeaderEvent(boolean isActive, int kraftControllerId, int kraftControllerEpoch) {
            this.isActive = isActive;
            this.kraftControllerId = kraftControllerId;
            this.kraftControllerEpoch = kraftControllerEpoch;
        }

        @Override
        public void run() throws Exception {
            // We can either the the active controller or just resigned being the controller.
            switch (migrationState) {
                case UNINITIALIZED:
                    // Poll and retry after initialization
                    long deadline = time.nanoseconds() + NANOSECONDS.convert(10, SECONDS);
                    eventQueue.scheduleDeferred(
                        "poll",
                        new EventQueue.DeadlineFunction(deadline),
                        this);
                    break;
                default:
                    if (!isActive) {
                        apply("KRaftLeaderEvent is active", state -> state.mergeWithControllerState(ZkControllerState.EMPTY));
                        transitionTo(MigrationState.INACTIVE);
                    } else {
                        // Apply the new KRaft state
                        apply("KRaftLeaderEvent not active", state -> state.withNewKRaftController(kraftControllerId, kraftControllerEpoch));
                        // Instead of doing the ZK write directly, schedule as an event so that we can easily retry ZK failures
                        transitionTo(MigrationState.BECOME_CONTROLLER);
                    }
                    break;
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("Had an exception in " + this.getClass().getSimpleName(), e);
        }
    }

    class BecomeZkLeaderEvent extends ZkWriteEvent {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case BECOME_CONTROLLER:
                    ZkControllerState zkControllerState = client.claimControllerLeadership(
                        recoveryState.kraftControllerId(), recoveryState.kraftControllerEpoch());
                    apply("BecomeZkLeaderEvent", state -> state.mergeWithControllerState(zkControllerState));
                    if (!recoveryState.zkMigrationComplete()) {
                        transitionTo(MigrationState.WAIT_FOR_BROKERS);
                    } else {
                        transitionTo(MigrationState.DUAL_WRITE);
                    }
                    break;
                default:
                    // Ignore the event as we're not trying to become controller anymore.
                    break;
            }
        }
    }

    class WaitForBrokersEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            switch (migrationState) {
                case WAIT_FOR_BROKERS:
                    Set<Integer> brokers = getBrokersFromTopicAssignments();
                    if (listener.zkBrokersReadyForMigration(brokers)) {
                        brokerIdsFromTopicAssignments = null;
                        transitionTo(MigrationState.ZK_MIGRATION);
                    }
                    break;
                default:
                    // State has change. Nothing to do.
                    break;
            }
        }
    }

    class PeriodicSendRequestsToBrokersEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            try {
                switch (migrationState) {
                    case DUAL_WRITE:
                        client.sendRequestsForBrokersFromImage(recoveryState.kraftControllerEpoch());
                        break;
                    default:
                        break;
                }
            } finally {
                // Poll again after some time
                long deadline = time.nanoseconds() + NANOSECONDS.convert(30, SECONDS);
                eventQueue.scheduleDeferred(
                    "periodicRpcRequests",
                    new EventQueue.DeadlineFunction(deadline),
                    new PeriodicSendRequestsToBrokersEvent());
            }
        }
    }

    private final Time time;
    private final Logger log;
    private final int nodeId;
    private final MigrationClient client;
    private final KafkaEventQueue eventQueue;
    private volatile MigrationState migrationState;
    private volatile MigrationRecoveryState recoveryState;
    private volatile Set<Integer> brokerIdsFromTopicAssignments;
    private final Map<Integer, ZkBrokerRegistration> zkBrokerRegistrations = new HashMap<>();
    private final MetadataLogListener listener = new MetadataLogListener();
    private ZkMetadataConsumer consumer;

    public KRaftMigrationDriver(int nodeId, MigrationClient client) {
        this.nodeId = nodeId;
        this.time = Time.SYSTEM;
        this.log = LoggerFactory.getLogger(KRaftMigrationDriver.class); // TODO use LogContext
        this.migrationState = MigrationState.UNINITIALIZED;
        this.recoveryState = MigrationRecoveryState.EMPTY;
        this.client = client;
        this.brokerIdsFromTopicAssignments = null;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, new LogContext("KRaftMigrationDriver"), "kraft-migration");
        client.initializeForBrokerRpcs(this);
    }

    private void initializeMigrationState() {
        log.info("Recovering migration state");
        apply("Recovery", client::getOrCreateMigrationRecoveryState);
        String maybeDone = recoveryState.zkMigrationComplete() ? "done" : "not done";
        log.info("Recovered migration state {}. ZK migration is {}.", recoveryState, maybeDone);
        // TODO: KRaft Migration should first make sure all quorum controller nodes
        //  are ready for migration. So the next state should be
        //  WAIT_FOR_CONTROLLER_QUORUM
        transitionTo(MigrationState.INACTIVE);
    }

    private Set<Integer> getBrokersFromTopicAssignments() {
        if (brokerIdsFromTopicAssignments == null) {
            brokerIdsFromTopicAssignments = client.readBrokerIdsFromTopicAssignments();
        }
        return brokerIdsFromTopicAssignments;
    }

    public void setMigrationCallback(ZkMetadataConsumer consumer) {
        this.consumer = consumer;
    }

    private MigrationRecoveryState migrationState(long metadataOffset, long metadataEpoch) {
        return new MigrationRecoveryState(recoveryState.kraftControllerId(), recoveryState.kraftControllerEpoch(),
                metadataOffset, metadataEpoch, time.milliseconds(), recoveryState.migrationZkVersion(), recoveryState.controllerZkVersion());
    }

    public void start() {
        eventQueue.prepend(new PollEvent());
    }

    public void shutdown() throws InterruptedException {
        eventQueue.close();
    }

    public KRaftMetadataListener listener() {
        return listener;
    }

    private void apply(String name, Function<MigrationRecoveryState, MigrationRecoveryState> stateMutator) {
        MigrationRecoveryState beforeState = KRaftMigrationDriver.this.recoveryState;
        MigrationRecoveryState afterState = stateMutator.apply(beforeState);
        log.debug("{} transitioned from {} to {}", name, beforeState, afterState);
        KRaftMigrationDriver.this.recoveryState = afterState;
    }

    private void transitionTo(MigrationState newState) {
        // TODO enforce a state machine
        // The next poll event will pick up the new state.
        if (newState == MigrationState.DUAL_WRITE) {
            // When being transitioned to DUAL_WRITE, first generate RPCs to the brokers.
            eventQueue.prepend(new SendRPCsToBrokersEvent(Optional.empty()));
        }
        migrationState = newState;
    }
}
