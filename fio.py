# Copyright 2014: Mirantis Inc.
# All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.

import argparse
import collections
import itertools
import json
import re
import subprocess
import sys


def _run_benchmark_once(iodepth, action, blocksize, timeout=30, size="1G"):
    p = subprocess.Popen(["fio", "--name=%s" % action,
                     "--rw=%s" % action,
                     "--blocksize=%s" % blocksize, "--direct=1",
                     "--ioengine=libaio", "--iodepth=%d" % iodepth,
                     "--filename=testo",
                     "--size=%s" % size,
                     "--timeout=%d" % timeout,
                     "--runtime=%d" % timeout,
                     "--output-format=json"
    ], stdout=subprocess.PIPE)

    return json.load(p.stdout)

_TYPE_SIZE_RE = re.compile("\d+[KGBM]?", re.I)
def _type_size(string):
    return _TYPE_SIZE_RE.match(string).group(0)
    

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run set of `fio` invocations and return result")
    parser.add_argument(
        "--iodepth", metavar="iodepth", nargs="+", type=int,
        help="I/O depths to test", dest="iodepths",
        default=[1, 2, 4, 8])
    parser.add_argument(
        "--action", metavar="action", nargs="+", type=str,
        help="actions to run", dest="actions",
        default=["read", "write", "randread", "randwrite", "randrw"])
    parser.add_argument(
        "--blocksize", metavar="blocksize", nargs="+", type=_type_size,
        help="actions to run", dest="blocksizes",
        default=["512", "4K", "64K"])
    parser.add_argument(
        "--timeout", metavar="timeout", type=int,
        help="actions to run", default=30)
    parser.add_argument(
        "--output", metavar="output", type=argparse.FileType("w"),
        help="actions to run", default=sys.stdout)


    return parser.parse_args()


_BenchmarkOption = collections.namedtuple(
    "BenchmarkOption", ["iodepth", "action", "blocksize"])
class BenchmarkOption(_BenchmarkOption):
    """Benchmark option for a single benchmark run."""
    pass

def run_benchmark_set(benchmark_set, **kwargs):
    """Runs a set of benchmarks and returns `fio` provided results.

    :param benchmark_set: an iterable that returns `BenchmarkOption` instances
    """

    # TODO(pboldin): prepare a jobs file to feed to fio and invoke
    #                it only once
    results = []
    for benchmark in benchmark_set:
        result = _run_benchmark_once(
            benchmark.iodepth, benchmark.action, benchmark.blocksize,
            **kwargs)
        result = result["jobs"][0]
        result["paramters"] = benchmark._asdict()
        results.append(result)

    return results


def main():
    args = _parse_args()
    benchmark_set = [BenchmarkOption(*product)
                     for product in itertools.product(
                            args.iodepths, args.actions,
                            args.blocksizes)]
                                
    results = run_benchmark_set(benchmark_set, timeout=args.timeout)

    args.output.write(json.dumps(results))


if __name__ == '__main__':
    main()
