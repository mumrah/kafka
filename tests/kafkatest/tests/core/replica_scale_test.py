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

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.kafka import KafkaService
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService

import json
import time


class ReplicaScaleTest(Test):
    def __init__(self, test_context):
        super(ReplicaScaleTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.kafka = KafkaService(self.test_context, num_nodes=8, zk=self.zk)
        self.workload_service = ProduceBenchWorkloadService(test_context, self.kafka)
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka, self.workload_service])
        self.active_topics = {"produce_bench_topic[0-9]": {"numPartitions": 34, "replicationFactor": 3}}
        self.inactive_topics = {"produce_bench_topic[10-19]": {"numPartitions": 34, "replicationFactor": 3}}

    def setUp(self):
        self.trogdor.start()
        self.zk.start()
        self.kafka.start()

    def teardown(self):
        self.zk.stop()
        self.trogdor.stop()
        # Need to increase the timeout due to partition count
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=False, timeout_sec=60)
        self.kafka.stop()

    @cluster(num_nodes=12)
    def test_100k_bench(self):
        spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                        self.workload_service.producer_node,
                                        self.workload_service.bootstrap_servers,
                                        target_messages_per_sec=1000,
                                        max_messages=1000000,
                                        producer_conf={},
                                        admin_client_conf={},
                                        common_client_conf={},
                                        inactive_topics=self.inactive_topics,
                                        active_topics=self.active_topics)
        workload1 = self.trogdor.create_task("100k-replicas-workload", spec)
        workload1.wait_for_done(timeout_sec=3600)
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))

    @cluster(num_nodes=12)
    def test_200k_replicas(self):

        t0 = time.time()
        for i in range(1000):
            topic = "topic-%04d" % i
            self.logger.info("Creating topic %s" % topic)
            topic_cfg = {
                "topic": topic,
                "partitions": 34,
                "replication-factor": 3,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg)

        t1 = time.time()
        self.logger.info("Time to create topics: %d" % (t1-t0))

        restart_times = []
        for node in self.kafka.nodes:
            t2 = time.time()
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=600)
            self.afka.start_node(node, timeout_sec=600)
            t3 = time.time()
            restart_times.append(t3-t2)
            self.logger.info("Time to restart %s: %d" % (node.name, t3-t2))

        self.logger.info("Restart times: %s" % restart_times)

        for i in range(1000):
            topic = "topic-%04d" % i
            self.logger.info("Deleting topic %s" % topic)
            topic_cfg = {
                "topic": topic,
                "partitions": 34,
                "replication-factor": 3,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg)
