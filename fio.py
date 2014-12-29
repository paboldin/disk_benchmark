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

import time
import argparse
import itertools
import json
import re
import subprocess
import sys
import Queue
import threading


class BenchmarkOption(object):
    def __init__(self, concurence, iodepth, action, blocksize, size):
        self.iodepth = iodepth
        self.action = action
        self.blocksize = blocksize
        self.concurence = concurence
        self.size = size
        self.direct_io = False
        self.use_hight_io_priority = True


class RunOptions(object):
    def __init__(self):
        # (executor_name, file_path, params)
        self.executors = []


class Results(object):
    def __init__(self):
        # depend on test utility used
        self.parameters = None
        self.type = None
        self.block_size = None
        self.concurence = None
        self.iodepth = None
        self.bw_dev = None
        self.bw_mean = None
        self.bw_max = None
        self.bw_min = None


def subprocess_executor(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE).stdout.read()
subprocess_executor.node = 'localhost'


def get_paramiko_executor(host, user, password):
    try:
        import paramiko
    except ImportError:
        msg = "Can't use ssh protocol. No paramiko module available"
        raise ValueError(msg)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=user, password=password)

    def paramiko_executor(cmd):
        stdin, stdout, stderr = ssh.exec_command(" ".join(cmd))
        return stdout.read()

    paramiko_executor.node = host

    return paramiko_executor


def _run_benchmark_once(executor, params, filename, timeout):
    cmd_line = ["fio",
                "--name=%s" % params.action,
                "--rw=%s" % params.action,
                "--blocksize=%s" % params.blocksize,
                "--ioengine=libaio",
                "--iodepth=%d" % params.iodepth,
                "--filename=%s" % filename,
                "--size={}".format(params.size),
                "--timeout=%d" % timeout,
                "--runtime=%d" % timeout,
                "--numjobs={}".format(params.concurence),
                "--output-format=json"]

    if params.direct_io:
        cmd_line.append("--direct=1")

    if params.use_hight_io_priority:
        cmd_line.append("--prio=6")

    return json.loads(executor(cmd_line))


_TYPE_SIZE_RE = re.compile("\d+[KGBM]?", re.I)


def _type_size(string):
    try:
        return _TYPE_SIZE_RE.match(string).group(0)
    except:
        msg = "{!r} don't looks like size-description string".format(string)
        raise ValueError(msg)


def _parse_args(args):
    parser = argparse.ArgumentParser(
        description="Run set of `fio` invocations and return result")
    parser.add_argument(
        "--iodepth", metavar="IODEPTHS", nargs="+", type=int,
        help="I/O depths to test", default=[1, 2, 4, 8])
    parser.add_argument(
        '-a', "--action", metavar="ACTIONS", nargs="+", type=str,
        help="actions to run",
        default=["read", "write", "randread", "randwrite"])
    parser.add_argument(
        "--blocksize", metavar="BLOCKSIZE", nargs="+", type=_type_size,
        help="actions to run", default=["512", "4K", "64K"])
    parser.add_argument(
        "--timeout", metavar="TIMEOUT", type=int,
        help="runtime of a single run", default=30)
    parser.add_argument(
        "--total-time", metavar="TOTAL_TIME", type=int,
        help="total execution time", default=None)
    parser.add_argument(
        "--output", metavar="OUTPUT", type=argparse.FileType("w"),
        help="filename to output", default=sys.stdout)
    parser.add_argument(
        "--iosize", metavar="SIZE", type=_type_size,
        help="file size", default=None)
    parser.add_argument(
        '-f', "--format", metavar="OUT_TYPE",
        help="output format", choices=['plain', 'table'], default='plain')
    parser.add_argument(
        "executors", metavar="executors",
        help="all executors", nargs='+')

    return parser.parse_args(args)


def run_benchmark(executor, benchmark, filename, timeout):
    job_output = _run_benchmark_once(executor,
                                     benchmark,
                                     filename,
                                     timeout)
    job_output = job_output["jobs"][0]
    res = Results()
    res.parameters = benchmark.__dict__
    res.type = benchmark.action

    if benchmark.action in ('write', 'randwrite'):
        raw_result = job_output['write']
    else:
        raw_result = job_output['read']

    res.block_size = benchmark.blocksize
    res.concurence = benchmark.concurence
    res.iodepth = benchmark.iodepth

    for field in 'bw_dev bw_mean bw_max bw_min'.split():
        setattr(res, field, raw_result[field])

    return res


def run_benchmark_th(res_q, executor, benchmark, filename, timeout):
    try:
        res = run_benchmark(executor, benchmark, filename, timeout)
    except:
        res = None
    res_q.put((executor, res))


def run_benchmark_set(executors, benchmark_set, timeout=30):
    """Runs a set of benchmarks and returns `fio` provided results.

    :param benchmark_set: an iterable that returns `BenchmarkOption` instances
    """

    # TODO(pboldin): prepare a jobs file to feed to fio and invoke
    #                it only once

    for benchmark in benchmark_set:
        q = Queue.Queue()
        threads = []
        stime = time.time()
        for (executor, filename) in executors:
            params = (q, executor, benchmark, filename, timeout)
            th = threading.Thread(None, run_benchmark_th, None, params)
            th.daemon = True
            threads.append(th)
            th.start()

        result = Results()
        for count, th in enumerate(threads):
            th.join()
            executor, th_res = q.get()

            if th_res is not None:
                print "At +", int(time.time() - stime), "sec ",
                print "get res from", executor.node,
                print "{}~{}".format(int(th_res.bw_mean), int(th_res.bw_dev))

                if result.block_size is None:
                    result.type = th_res.type
                    result.block_size = th_res.block_size
                    result.concurence = th_res.concurence
                    result.iodepth = th_res.iodepth
                    result.bw_mean = th_res.bw_mean
                    result.bw_max = th_res.bw_max
                    result.bw_min = th_res.bw_min
                    result.bw_dev = th_res.bw_dev
                else:
                    assert result.type == th_res.type
                    assert result.block_size == th_res.block_size
                    assert result.concurence == th_res.concurence
                    assert result.iodepth == th_res.iodepth

                    # ????
                    result.bw_mean += th_res.bw_mean

                    result.bw_max = max(result.bw_max, th_res.bw_max)
                    result.bw_min = min(result.bw_min, th_res.bw_min)

                    sq_dev = (result.bw_dev ** 2 * count + th_res.bw_dev ** 2)
                    result.bw_dev = (sq_dev / (count + 1)) ** 0.5
            else:
                print "Node", executor.node, "fails to execute fio"

        yield result


def create_executor(uri):
    exec_name, params = uri.split("://", 1)
    if exec_name == 'local':
        return (subprocess_executor, params)
    elif exec_name == 'ssh':
        user_password, host_path = params.split("@", 1)
        host, path = host_path.split(":", 1)
        user, password = user_password.split(":", 1)
        return (get_paramiko_executor(host, user, password), path)
    else:
        raise ValueError("Can't instantiate executor from {!r}".format(uri))


def main(args):
    args = _parse_args(args)

    params = [1], args.iodepth, args.action, args.blocksize, [args.iosize]
    all_combinations = itertools.product(*params)

    benchmark_set = [BenchmarkOption(*product)
                     for product in all_combinations]

    timeout = args.timeout
    if args.total_time:
        if timeout:
            print "Both timeout and total_time parameters provided.",
            print "timeout option will be ignored."
        timeout = args.total_time / len(benchmark_set)

    executors = map(create_executor, args.executors)

    results = run_benchmark_set(executors,
                                benchmark_set,
                                timeout=timeout)

    if args.format == 'table':
        try:
            import texttable
            table = texttable.Texttable()
            table.set_deco(texttable.Texttable.HEADER)
            fields = 'Type BlockSize Concurence Iodepth BW_MEAN~BW_DEV'.split()
            table.set_cols_align(["l"] * len(fields))
            table.add_row(fields)
        except ImportError:
            print "texttable module isn't available, failback to plain text"
            table = None
    else:
        table = None

    for res in results:
        if table is not None:
            table.add_row([
                res.type, res.block_size, res.concurence,
                res.iodepth, "{}~{}".format(int(res.bw_mean),
                                            int(res.bw_dev))])
        else:
            print res.type, res.block_size, res.concurence,
            print res.iodepth, "{}~{}".format(int(res.bw_mean),
                                              int(res.bw_dev))

    if table is not None:
        print table.draw() + "\n"

    return 0


if __name__ == '__main__':
    exit(main(sys.argv[1:]))
