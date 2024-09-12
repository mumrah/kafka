#!/bin/bash
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

SLEEP_MINUTES=$(($TIMEOUT_MINUTES-5))
echo "Dumping threads in $SLEEP_MINUTES minutes"
sleep $(($SLEEP_MINUTES*60));

echo "Dumping threads now..."
sleep 5;

for GRADLE_WORKER_PID in `jps | grep GradleWorkerMain | awk -F" " '{print $1}'`;
do
  echo "Thread Dump for GradleWorkerMain pid $GRADLE_WORKER_PID";
  kill -3 $GRADLE_WORKER_PID;
  sleep 10;
done;
