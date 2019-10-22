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

from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService

import time


class ReplicaScaleTest(Test):
    def __init__(self, test_context):
        super(ReplicaScaleTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, num_nodes=3)

    def setUp(self):
        self.zk.start()

    def teardown(self):
        self.zk.stop()

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(ReplicaScaleTest, self).min_cluster_size() + 8

    @cluster(num_nodes=12)
    def test_200k_replicas(self):
        kafka = KafkaService(self.test_context, num_nodes=8, zk=self.zk)
        kafka.start()

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
            kafka.create_topic(topic_cfg)

        t1 = time.time()
        self.logger.info("Time to create topics: %d" % (t1-t0))

        restart_times = []
        for node in kafka.nodes:
            t2 = time.time()
            kafka.stop_node(node, clean_shutdown=True, timeout_sec=600)
            kafka.start_node(node, timeout_sec=600)
            t3 = time.time()
            restart_times.append(t3-t2)
            self.logger.info("Time to restart %s: %d" % (node.name, t3-t2))

        self.logger.info("Restart times: %s" % restart_times)

        for node in kafka.nodes:
            kafka.stop_node(node, clean_shutdown=False, timeout_sec=60)
        kafka.stop()
