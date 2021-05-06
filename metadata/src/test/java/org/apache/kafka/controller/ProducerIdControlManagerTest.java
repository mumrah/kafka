/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.ProducerIdRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProducerIdControlManagerTest {

    private ProducerIdControlManager producerIdControlManager;

    @BeforeEach
    public void setUp() {
        final LogContext logContext = new LogContext();
        final MockTime time = new MockTime();
        final Random random = new Random();
        final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        final ClusterControlManager clusterControl = new ClusterControlManager(
            logContext, time, snapshotRegistry, 1000,
            new SimpleReplicaPlacementPolicy(random));

        clusterControl.activate();
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(1);
        brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092).
            setName("PLAINTEXT").
            setHost("example.com"));
        clusterControl.replay(brokerRecord);
        this.producerIdControlManager = new ProducerIdControlManager(clusterControl, snapshotRegistry);
    }

    @Test
    public void testInitialResult() {
        ControllerResult<ResultOrError<ProducerIdControlManager.ProducerIdRange>> result =
            producerIdControlManager.generateNextProducerId(1, 100);
        assertEquals(0, result.response().result().producerIdStart());
        assertEquals(1000, result.response().result().producerIdLen());
        ProducerIdRecord record = (ProducerIdRecord) result.records().get(0).message();
        assertEquals(1000, record.producerIdEnd());
    }

    @Test
    public void testMonotonic() {
        producerIdControlManager.replay(
            new ProducerIdRecord()
                .setBrokerId(1)
                .setBrokerEpoch(100)
                .setProducerIdEnd(42));

        ProducerIdControlManager.ProducerIdRange range =
                producerIdControlManager.generateNextProducerId(1, 100).response().result();
        assertEquals(42, range.producerIdStart());

        // Can't go backwards in Producer IDs
        assertThrows(RuntimeException.class, () -> {
            producerIdControlManager.replay(
                new ProducerIdRecord()
                    .setBrokerId(1)
                    .setBrokerEpoch(100)
                    .setProducerIdEnd(40));
        }, "Producer ID range must only increase");
        range = producerIdControlManager.generateNextProducerId(1, 100).response().result();
        assertEquals(42, range.producerIdStart());

        // Gaps in the ID range are okay.
        producerIdControlManager.replay(
            new ProducerIdRecord()
                .setBrokerId(1)
                .setBrokerEpoch(100)
                .setProducerIdEnd(50));
        range = producerIdControlManager.generateNextProducerId(1, 100).response().result();
        assertEquals(50, range.producerIdStart());
    }

    @Test
    public void testUnknownBrokerOrEpoch() {
        ControllerResult<ResultOrError<ProducerIdControlManager.ProducerIdRange>> result;

        result = producerIdControlManager.generateNextProducerId(99, 0);
        assertEquals(Errors.STALE_BROKER_EPOCH, result.response().error().error());

        result = producerIdControlManager.generateNextProducerId(1, 99);
        assertEquals(Errors.STALE_BROKER_EPOCH, result.response().error().error());
    }

    @Test
    public void testMaxValue() {
        producerIdControlManager.replay(
            new ProducerIdRecord()
                .setBrokerId(1)
                .setBrokerEpoch(100)
                .setProducerIdEnd(Long.MAX_VALUE - 1));

        ControllerResult<ResultOrError<ProducerIdControlManager.ProducerIdRange>> result =
                producerIdControlManager.generateNextProducerId(1, 100);
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, result.response().error().error());
    }
}
