# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import random
import time

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import config_property
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int


class ReassignPartitionsTest(ProduceConsumeValidateTest):
    """
    These tests validate partition reassignment.
    Create a topic with few partitions, load some data, trigger partition re-assignment with and without broker failure,
    check that partition re-assignment can complete and there is no data loss.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReassignPartitionsTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.num_partitions = 20
        self.zk = ZookeeperService(test_context, num_nodes=1)
        # We set the min.insync.replicas to match the replication factor because
        # it makes the test more stringent. If min.isr = 2 and
        # replication.factor=3, then the test would tolerate the failure of
        # reassignment for upto one replica per partition, which is not
        # desirable for this test in particular.
        self.kafka = KafkaService(test_context, num_nodes=4, zk=self.zk,
                                  server_prop_overides=[
                                      [config_property.LOG_ROLL_TIME_MS, "5000"],
                                      [config_property.LOG_RETENTION_CHECK_INTERVAL_MS, "5000"]
                                  ],
                                  topics={self.topic: {
                                      "partitions": self.num_partitions,
                                      "replication-factor": 3,
                                      'configs': {
                                          "min.insync.replicas": 3,
                                      }}
                                  })
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ReassignPartitionsTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def clean_bounce_some_brokers(self):
        """Bounce every other broker"""
        for node in self.kafka.nodes[::2]:
            self.kafka.restart_node(node, clean_shutdown=True)

    def reassign_partitions(self, bounce_brokers):
        partition_info = self.kafka.parse_describe_topic(self.kafka.describe_topic(self.topic))
        self.logger.debug("Partitions before reassignment:" + str(partition_info))

        # jumble partition assignment in dictionary
        seed = random.randint(0, 2 ** 31 - 1)
        self.logger.debug("Jumble partition assignment with seed " + str(seed))
        random.seed(seed)
        # The list may still be in order, but that's ok
        shuffled_list = range(0, self.num_partitions)
        random.shuffle(shuffled_list)

        for i in range(0, self.num_partitions):
            partition_info["partitions"][i]["partition"] = shuffled_list[i]
        self.logger.debug("Jumbled partitions: " + str(partition_info))

        # send reassign partitions command
        self.kafka.execute_reassign_partitions(partition_info)

        if bounce_brokers:
            # bounce a few brokers at the same time
            self.clean_bounce_some_brokers()

        # Wait until finished or timeout
        wait_until(lambda: self.kafka.verify_reassign_partitions(partition_info),
                   timeout_sec=self.timeout_sec, backoff_sec=.5)

    def move_start_offset(self):
        """We move the start offset of the topic by writing really old messages
        and waiting for them to be cleaned up.
        """
        producer = VerifiableProducer(self.test_context, 1, self.kafka, self.topic,
                                      throughput=-1, enable_idempotence=True,
                                      create_time=1000)
        producer.start()
        wait_until(lambda: producer.num_acked > 0,
                   timeout_sec=30,
                   err_msg="Failed to get an acknowledgement for %ds" % 30)
        # Wait 8 seconds to let the topic be seeded with messages that will
        # be deleted. The 8 seconds is important, since we should get 2 deleted
        # segments in this period based on the configured log roll time and the
        # retention check interval.
        time.sleep(8)
        producer.stop()
        self.logger.info("Seeded topic with %d messages which will be deleted" %\
                         producer.num_acked)

        # Wait until the earliest and latest offsets match for all partitions. This will ensure that we have deleted
        # the segments containing the "old" records created above.
        attempt = 0
        all_offsets_match = True
        while attempt < 10:
            earliest_offset_output = self.kafka.get_offset_shell(self.topic, None, 1000, 1, -2)
            earliest_offsets = {}
            for line in earliest_offset_output.strip().split("\n"):
                topic, partition, offset = line.split(":")
                earliest_offsets["%s-%s" % (topic, partition)] = offset

            latest_offset_output = self.kafka.get_offset_shell(self.topic, None, 1000, 1, -1)
            latest_offsets = {}
            for line in latest_offset_output.strip().split("\n"):
                topic, partition, offset = line.split(":")
                latest_offsets["%s-%s" % (topic, partition)] = offset

            self.logger.debug("Earliest Offsets: %s" % json.dumps(earliest_offsets, indent=2))
            self.logger.debug("Latest Offsets: %s" % json.dumps(latest_offsets, indent=2))

            all_offsets_match = True
            for tp, offset in earliest_offsets.items():
                if latest_offsets[tp] != offset:
                    all_offsets_match = False
                    attempt += 1
                    break

            if all_offsets_match:
                break
            time.sleep(5)

        assert all_offsets_match, "Timed out waiting for start offsets to match end offsets"

    @cluster(num_nodes=8)
    @matrix(bounce_brokers=[True, False],
            reassign_from_offset_zero=[True, False])
    def test_reassign_partitions(self, bounce_brokers, reassign_from_offset_zero):
        """Reassign partitions tests.
        Setup: 1 zk, 4 kafka nodes, 1 topic with partitions=20, replication-factor=3,
        and min.insync.replicas=3

            - Produce messages in the background
            - Consume messages in the background
            - Reassign partitions
            - If bounce_brokers is True, also bounce a few brokers while partition re-assignment is in progress
            - When done reassigning partitions and bouncing brokers, stop producing, and finish consuming
            - Validate that every acked message was consumed
            """
        self.kafka.start()
        if not reassign_from_offset_zero:
            self.move_start_offset()

        self.producer = VerifiableProducer(self.test_context, self.num_producers,
                                           self.kafka, self.topic,
                                           throughput=self.producer_throughput,
                                           enable_idempotence=True)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers,
                                        self.kafka, self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int)

        self.enable_idempotence=True
        self.run_produce_consume_validate(core_test_action=lambda: self.reassign_partitions(bounce_brokers))
