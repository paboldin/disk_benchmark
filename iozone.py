import re
import time
import subprocess

from common import BenchmarkOption, RunOptions, Results

# run iozone disk io tests
# see http://www.iozone.org/ for more detailes


def install_iozone_source(ver="3_397"):
    "install iozone from source"
    if not exists('/tmp/iozone'):
        with cd('/tmp'):
            run('rm -f iozone{0}.tar'.format(ver))
            run('rm -rf iozone{0}'.format(ver))
            run('wget http://www.iozone.org/src/current/iozone{0}.tar'\
                        .format(ver))
            run('tar xf iozone{0}.tar'.format(ver))
            with cd('iozone{0}/src/current'.format(ver)):
                run('make linux-AMD64')
                run('cp iozone /tmp')
    return '/tmp/iozone'

def install_iozone():
    "install iozone package"
    return which('iozone')

class IOZoneParser(object):
    "class to parse iozone results"
    
    start_tests = re.compile(r"^\s+KB\s+reclen\s+")
    resuts = re.compile(r"[\s0-9]+")
    mt_iozone_re = re.compile(r"\s+Children see throughput " + \
                    "for\s+\d+\s+(?P<cmd>.*?)\s+=\s+(?P<perf>[\d.]+)\s+KB/sec")

    cmap = {'initial writers' : 'write',
        'rewriters' : 'rewrite',
        'initial readers':'read',
        're-readers' : 'reread',
        'random readers' : 'random read',
        'random writers' : 'random write'
        }


    string1 = "                                              random  random    " + \
          "bkwd   record   stride                                   "
    string2 = "KB  reclen   write rewrite    read    reread    read   write    " + \
          "read  rewrite     read   fwrite frewrite   fread  freread"

    @classmethod
    def apply_parts(cls, parts, string, sep=' \t\n'):
        add_offset = 0
        for part in parts:
            _, start, stop = part
            start += add_offset
            add_offset = 0
            
            while stop + add_offset < len(string) and \
                        string[stop + add_offset] not in sep:
                add_offset += 1
                
            yield part, string[start:stop + add_offset]

    @classmethod
    def make_positions(cls):
        items = [i for i in cls.string2.split() if i]
        
        pos = 0
        cls.positions = []
        
        for item in items:
            npos = cls.string2.index(item, 0 if pos == 0 else pos + 1)
            cls.positions.append([item, pos, npos + len(item)])
            pos = npos + len(item)
        
        for itm, val in cls.apply_parts(cls.positions, cls.string1):
            if val.strip():
                itm[0] = val.strip() + " " + itm[0]
        
    @classmethod
    def parse_iozone_res(cls, res, mthreads = False):
        parsed_res = None
        
        sres = res.split('\n')
        
        if not mthreads:
            for pos, line in enumerate(sres[1:]):
                if line.strip() == cls.string2 and \
                            sres[pos].strip() == cls.string1.strip():
                    add_pos = line.index(cls.string2)
                    parsed_res = {}
                    
                    npos = [(name, start + add_pos, stop + add_pos)
                                for name, start, stop in cls.positions]
                    
                    for itm, res in cls.apply_parts(npos, sres[pos + 2]):
                        if res.strip() != '':
                            parsed_res[itm[0]] = int(res.strip())

                    del parsed_res['KB']
                    del parsed_res['reclen']
        else:
            parsed_res = {}
            for line in sres:
                rr = cls.mt_iozone_re.match(line)
                if rr is not None:
                    cmd = rr.group('cmd')
                    key = cls.cmap.get(cmd, cmd)
                    perf = int(float(rr.group('perf')))
                    parsed_res[key] = perf
        return parsed_res


IOZoneParser.make_positions()


def do_run_iozone(path, size, bsize, threads,
                  sync=True,
                  seq_write=True,
                  random_write=False):

    iozone_exec = 'iozone'

    threads = int(threads)
    if 1 != threads:
        threads_opt = '-t {0}'.format(threads)
        path_opt = "-F " + " ".join(path % i for i in range(threads))
    else:
        threads_opt = ''
        if '%d' in path:
            path = path % (0,)
        path_opt = '-f {0}'.format(path)

    if sync:
        sync = '-o'
    else:
        sync = ''

    tests = []

    if seq_write:
        tests.append("0")
    if random_write:
        tests.append("2")

    tests = " ".join("-i " + test for test in tests)

    cmd = '{iozone_exec} {sync} {tests} {th} -s {size} -r {bsize} {fpath}'.\
          format(iozone_exec=iozone_exec,
                 th=threads_opt,
                 size=size,
                 bsize=bsize,
                 tests=tests,
                 fpath=path_opt,
                 sync=sync)

    # res = executor(cmd)
    res = subprocess.check_output(cmd, shell=True)

    parsed_res = IOZoneParser.parse_iozone_res(res, threads > 1)

    parsed_res['cmd'] = cmd
    parsed_res['fpath'] = parsed_res['cmd'].split()[-1]

    return parsed_res


def iozone(path, mark,
           size=10, bsize=4, threads=1,
           results=None, **tests):
    """
    run iozone, parse results and return dict of results
    params:
    mark : '/' separated string
    size : test file size in kb
    bsize : test block size in kb
    thread : number of threads to run
    remote_sensor : string with sensor type to run on remote host
                    for more info see fablib.recipes.sensor docs

    local_sensor : string with sensor type to run on local host
                    for more info see fablib.recipes.sensor docs

    results = {} or None. Will put results in this dict with key of
                current host name
    """

    size = int(size)
    bsize = int(bsize)
    threads = int(threads)

    parsed_res = do_run_iozone(path, size, bsize, threads, **tests)

    parsed_res['bsize'] = bsize
    parsed_res['fsize'] = size
    parsed_res['time'] = int(time.time())
    parsed_res['host'] = 'localhost'
    parsed_res['threads'] = threads
    parsed_res['mark'] = mark.split('/')

    return parsed_res

print iozone("/tmp/xxx.bin", '1', size=200, random_write=True, sync=True)

# def run_iozone_once(executor, params, filename, timeout, fio_path='fio'):
#     cmd_line = [fio_path,
#                 "--name=%s" % params.action,
#                 "--rw=%s" % params.action,
#                 "--blocksize=%s" % params.blocksize,
#                 "--ioengine=libaio",
#                 "--iodepth=%d" % params.iodepth,
#                 "--filename=%s" % filename,
#                 "--size={0}".format(params.size),
#                 "--timeout=%d" % timeout,
#                 "--runtime=%d" % timeout,
#                 "--numjobs={0}".format(params.concurence),
#                 "--output-format=json"]

#     if params.direct_io:
#         cmd_line.append("--direct=1")

#     if params.use_hight_io_priority:
#         cmd_line.append("--prio=6")

#     raw_out = executor(cmd_line)
#     for counter in range(100):
#         fname = "/tmp/fio_raw_{0}_{1}.json".format(time.time(), counter)
#         if not os.path.exists(fname):
#             break
#     open(fname, "w").write(raw_out)
#     return json.loads(raw_out)

# def run_fio(executor, benchmark, filename, timeout, fio_path='fio'):
#     job_output = run_fio_once(executor,
#                               benchmark,
#                               filename,
#                               timeout,
#                               fio_path)
#     job_output = job_output["jobs"][0]
#     res = Results()
#     res.parameters = benchmark.__dict__
#     res.type = benchmark.action

#     if benchmark.action in ('write', 'randwrite'):
#         raw_result = job_output['write']
#     else:
#         raw_result = job_output['read']

#     res.block_size = benchmark.blocksize
#     res.concurence = benchmark.concurence
#     res.iodepth = benchmark.iodepth

#     for field in 'bw_dev bw_mean bw_max bw_min'.split():
#         setattr(res, field, raw_result[field])

#     return res

