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


def module_from_class(class_name: str):
    toks = class_name.split(".")
    if toks[0] in ("unit", "integration"):
        return toks[2]
    if toks[0] == "kafka":
        if len(toks) == 2:
            return "kafka"
        else:
            return toks[1]
    if toks[0:3] == ["org", "apache", "kafka"]:
        return toks[3]

    return "unknown"


def all_tests_to_yaml(file_path: str, out_dir: str):
    method_matcher = re.compile("([a-zA-Z_$][a-zA-Z0-9]+).*")

    all_tests = {}
    with open(file_path, "r") as fp:
        for line in fp:
            test_tokens = line.split("#", maxsplit=1)
            class_name = test_tokens[0]
            module = module_from_class(class_name)
            if module not in all_tests:
                all_tests[module] = OrderedDict()
            if class_name not in all_tests[module]:
                all_tests[module][class_name] = set()
            method = test_tokens[1].rstrip("()")
            m = method_matcher.match(method)
            all_tests[module][class_name].add(m.group(1))

    if not os.path.exists(out_dir):
        logger.debug(f"Creating output directory {out_dir}.")
        os.makedirs(out_dir)

    for module, tests in all_tests.items():
        sorted_tests = {}
        count = 0
        for test, methods in tests.items():
            sorted_methods = sorted(methods)
            count += len(sorted_methods)
            sorted_tests[test] = sorted_methods

        out_path = os.path.join(out_dir, f"{module}-tests.yaml")
        logger.debug(f"Writing {count} tests for {module} into {out_path}.")
        stream = open(out_path, "w")
        yaml.dump(sorted_tests, stream)


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
    parser = argparse.ArgumentParser(description="Convert test suite to and from YAML.")
    subparsers = parser.add_subparsers(dest="command")
    to_yaml_parser = subparsers.add_parser("to-yaml", help="Convert the module test suite data to YAML files")
    to_yaml_parser.add_argument("--path",
                                required=False,
                                default="**/build/module-tests.txt",
                                help="Path to module-tests.txt files. Glob patterns are supported.")
    to_yaml_parser.add_argument("--yaml-output-dir",
                                required=False,
                                default="data/module-tests",
                                help="Directory to output YAML files")

    from_yaml_parser = subparsers.add_parser("from-yaml", help="Convert the YAML files to a test suite descriptor")
    from_yaml_parser.add_argument("--path",
                                  required=False,
                                  default="data/module-tests/*.yaml",
                                  help="Path to module YAML files. Glob patterns are supported.")
    from_yaml_parser.add_argument("--output-file",
                                  required=False,
                                  default="data/all-tests.txt",
                                  help="Output file location")

    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    args = parser.parse_args()
    if args.command == "to-yaml":
        all_tests_to_yaml(args.path, args.yaml_output_dir)
        exit(0)
    elif args.command == "from-yaml":
        yaml_to_all_tests(args.path, args.output_file)
        exit(0)
    else:
        print(f"Unknown sub-command: {args.command}")
        exit(1)
