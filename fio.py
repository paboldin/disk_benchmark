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
import json
import os.path

from common import Results


def run_fio_once(executor, params, filename, timeout, fio_path='fio',
                 sync_obj=None):

    cmd_line = [fio_path,
                "--name=%s" % params.action,
                "--rw=%s" % params.action,
                "--blocksize=%sk" % params.blocksize,
                "--ioengine=libaio",
                "--iodepth=%d" % params.iodepth,
                "--filename=%s" % filename,
                "--size={0}k".format(params.size),
                "--timeout=%d" % timeout,
                "--runtime=%d" % timeout,
                "--numjobs={0}".format(params.concurence),
                "--output-format=json",
                "--sync=" + ('1' if params.sync else '0')]

    if params.direct_io:
        cmd_line.append("--direct=1")

    if params.use_hight_io_priority:
        cmd_line.append("--prio=0")

    if sync_obj:
        sync_obj.wait()

    raw_out = executor(cmd_line)
    for counter in range(100):
        fname = "/tmp/fio_raw_{0}_{1}.json".format(time.time(), counter)
        if not os.path.exists(fname):
            break
    open(fname, "w").write(raw_out)
    return json.loads(raw_out)


def run_fio(executor, benchmark, filename, timeout, fio_path='fio',
            sync_obj=None):

    job_output = run_fio_once(executor,
                              benchmark,
                              filename,
                              timeout,
                              fio_path,
                              sync_obj=sync_obj)
    job_output = job_output["jobs"][0]
    res = Results()

    if benchmark.action in ('write', 'randwrite'):
        raw_result = job_output['write']
    else:
        raw_result = job_output['read']

    for field in 'bw_dev bw_mean bw_max bw_min'.split():
        setattr(res, field, raw_result[field])

    return res
