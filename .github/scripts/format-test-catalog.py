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

import argparse
from collections import OrderedDict
from glob import glob
import logging
import os
import re
import sys

import yaml


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def yaml_to_all_tests(glob_path: str, out_file: str):
    yamls = glob(pathname=glob_path, recursive=True)
    logger.debug(f"Found {len(yamls)} YAML files")

    with open(out_file, "w") as fp:
        for yaml_file in yamls:
            with open(yaml_file, "r") as yamp_fp:
                tests = yaml.safe_load(yamp_fp)
                for clazz, methods in tests.items():
                    for method in methods:
                        fp.write(f"{clazz}#{method}\n")

    logger.debug(f"Wrote to {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert the test catalog to a single text file")

    parser.add_argument("--path",
                        required=False,
                        default="test-catalog/*.yaml",
                        help="Path to module YAML files. Glob patterns are supported.")
    parser.add_argument("--output-file",
                        required=False,
                        default="data/all-tests.txt",
                        help="Output file location")

    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    args = parser.parse_args()
    yaml_to_all_tests(args.path, args.output_file)
