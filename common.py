import subprocess


class BenchmarkOption(object):
    def __init__(self, concurence, iodepth, action, blocksize, size):
        self.iodepth = iodepth
        self.action = action
        self.blocksize = blocksize
        self.concurence = concurence
        self.size = size
        self.direct_io = False
        self.use_hight_io_priority = True
        self.sync = False


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

    def __str__(self):
        frmt = "<{0} bsize={1} type={4} bw={2}~{3}>".format
        return frmt(self.__class__.__name__,
                    self.block_size,
                    self.bw_mean,
                    self.bw_dev,
                    self.type)

    def __repr__(self):
        return str(self)


def subprocess_executor(cmd):
    print "execute on localhost:", " ".join(cmd)
    return subprocess.Popen(cmd, stdout=subprocess.PIPE).stdout.read()
subprocess_executor.node = 'localhost'


def get_paramiko_executor(host, user, password, key_file=None):
    try:
        import paramiko
    except ImportError:
        msg = "Can't use ssh protocol. No paramiko module available"
        raise ValueError(msg)

    ssh = paramiko.SSHClient()
    ssh.load_host_keys('/dev/null')
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.known_hosts = None

    if password == '-':
        if key_file is None:
            raise ValueError("password is '-' and no key_file provided")
        else:
            ssh.connect(host, username=user, key_filename=key_file,
                        look_for_keys=False)
    else:
        ssh.connect(host, username=user, password=password)

    def paramiko_executor(cmd):
        print "Try to execute", " ".join(cmd)
        stdin, stdout, stderr = ssh.exec_command(" ".join(cmd))
        return stdout.read()

    paramiko_executor.node = host

    return paramiko_executor
