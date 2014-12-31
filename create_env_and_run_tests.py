import re
import os
import time
import subprocess

from novaclient.client import Client as n_client
from cinderclient.v1.client import Client as c_client

import fio


def ostack_get_creds():
    env = os.environ.get
    name = env('OS_USERNAME')
    passwd = env('OS_PASSWORD')
    tenant = env('OS_TENANT_NAME')
    auth_url = env('OS_AUTH_URL')
    return name, passwd, tenant, auth_url


def nova_connect():
    return n_client('1.1', *ostack_get_creds())


def create_keypair(nova, name, key_path):
    with open(key_path) as key:
        return nova.keypairs.create(name, key.read())


def create_volume(size, name=None, volid=[0]):
    cinder = c_client(*ostack_get_creds())
    name = 'ceph-test-{0}'.format(volid[0])
    volid[0] = volid[0] + 1
    vol = cinder.volumes.create(size=size, display_name=name)
    err_count = 0
    while vol.status != 'available':
        if vol.status == 'error':
            if err_count == 3:
                print "Fail to create volume"
                raise RuntimeError("Fail to create volume")
            else:
                err_count += 1
                cinder.volumes.delete(vol)
                time.sleep(1)
                vol = cinder.volumes.create(size=size, display_name=name)
                continue
        time.sleep(1)
        vol = cinder.volumes.get(vol.id)
    return vol


def wait_for_server_active(nova, server, timeout=240):
    t = time.time()
    while True:
        time.sleep(5)
        sstate = getattr(server, 'OS-EXT-STS:vm_state').lower()

        if sstate == 'active':
            return True

        print "Curr state is", sstate, "waiting for active"

        if sstate == 'error':
            return False

        if time.time() - t > timeout:
            return False

        server = nova.servers.get(server)


def get_or_create_floating_ip(nova, pool=None):
    ip_list = nova.floating_ips.list()
    ip_list = [ip for ip in ip_list if ip.instance_id is None]

    if pool is not None:
        ip_list = [ip for ip in ip_list if ip.pool == pool]

    if len(ip_list) > 0:
        # don't forget to import random
        return ip_list[0]
    else:
        return nova.floating_ips.create(pool)


def create_vms(nova, amount, keypair_name, vol_sz=1, img_name='TestVM'):
    fl = nova.flavors.find(ram=512)
    img = nova.images.find(name=img_name)
    srvs = []
    counter = 0

    for i in range(3):
        amount_left = amount - len(srvs)

        new_srvs = []
        for i in range(amount_left):
            print "creating server"
            srv = nova.servers.create("ceph-test-{0}".format(counter),
                                      flavor=fl, image=img,
                                      key_name=keypair_name)
            counter += 1
            new_srvs.append(srv)
            print srv

        deleted_servers = []
        for srv in new_srvs:
            if not wait_for_server_active(nova, srv):
                print "Server", srv.name, "fails to start. Kill it and",
                print " try again"

                nova.servers.delete(srv)
                deleted_servers.append(srv)
            else:
                srvs.append(srv)

        if len(deleted_servers) != 0:
            time.sleep(5)

    if len(srvs) != amount:
        print "ERROR: can't start required amount of servers. Exit"
        raise RuntimeError("Fail to create {0} servers".format(amount))

    result = {}
    for srv in srvs:
        print "wait till server be ready"
        wait_for_server_active(nova, srv)
        print "creating volume"
        vol = create_volume(vol_sz)
        print "attach volume to server"
        nova.volumes.create_server_volume(srv.id, vol.id, None)
        print "create floating ip"
        flt_ip = get_or_create_floating_ip(nova)
        print "attaching ip to server"
        srv.add_floating_ip(flt_ip)
        result[flt_ip.ip] = srv

    return result


def clear_all(nova):
    deleted_srvs = set()
    for srv in nova.servers.list():
        if re.match(r"ceph-test-\d+", srv.name):
            print "Deleting server", srv.name
            nova.servers.delete(srv)
            deleted_srvs.add(srv.id)

    while deleted_srvs != set():
        print "Waiting till all servers are actually deleted"
        all_id = set(srv.id for srv in nova.servers.list())
        if all_id.intersection(deleted_srvs) == set():
            print "Done, deleting volumes"
            break
        time.sleep(1)

    # wait till vm actually deleted

    cinder = c_client(*ostack_get_creds())
    for vol in cinder.volumes.list():
        if isinstance(vol.display_name, basestring):
            if re.match(r'ceph-test-\d+', vol.display_name):
                print "Deleting volume", vol.display_name
                cinder.volumes.delete(vol)

    print "Clearing done (yet some volumes may still deleting)"


def copy_fio(key_file, ip, fio_path, user):
    params = (key_file, fio_path, user, ip)
    cmd = 'scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i {0} {1} {2}@{3}:/tmp/fio'.format(*params)
    print "    " + cmd
    subprocess.check_call(cmd, shell=True)


def prepare_host(key_file, ip, fio_path, user='cirros'):
    print "Preparing host >"
    print "    Coping fio"
    copy_fio(key_file, ip, fio_path, user)

    def exec_on_host(cmd):
        print "    " + cmd
        subprocess.check_call(cmd_format(cmd), shell=True)

    cmd_format = "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i {0} {1}@{2} '{{0}}'".format(key_file, user, ip).format
    exec_on_host("sudo /usr/sbin/mkfs.ext4 /dev/vdb")
    exec_on_host("sudo /bin/mkdir /media/ceph")
    exec_on_host("sudo /bin/mount /dev/vdb /media/ceph")
    exec_on_host("sudo /bin/chmod a+rwx /media/ceph")


def run_fio_test(key_file, ips):
    class FIOParams(object):
        pass

    res_name = "/tmp/fio_res_{0}.txt".format(int(time.time()))
    with open(res_name, "w") as fd:
        fp = FIOParams()
        fp.output = fd
        fp.iodepth = [1, 4, 16, 64]
        fp.action = ['randwrite', 'randread']
        fp.blocksize = [512, 4096, 64 * 1024]
        fp.iosize = '3GB'
        fp.keyfile = key_file
        fp.fio = '/tmp/fio'
        fp.timeout = 120
        fp.total_time = None
        fp.executors = ["ssh://cirros:-@{0}:/media/ceph/test.ceph".format(ip)
                        for ip in ips]
        fp.format = 'plain'
        fio.do_main(fp)


def main():
    nova = nova_connect()
    clear_all(nova)
    # create_keypair(nova, 'koder', '/root/os_rsa.pub')
    # create_vms(nova, 1, 'ceph-test')
    ips = ['172.16.0.128']
    for ip, host in create_vms(nova, 1, 'ceph-test', vol_sz=4, img_name='TestVM').items():
        #create_vms(nova, 1, 'koder', img_name='cirros-0.3.2-x86_64-uec')
        prepare_host('os_rsa', ip, 'fio')
        ips.append(ip)
    print "All setup done! Ips =", " ".join(ips)
    print "Starting tests"
    run_fio_test('os_rsa', ips)

if __name__ == "__main__":
    exit(main())
