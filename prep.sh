#!/bin/bash
set -x
set -e

# apt-get install iozone3
# apt-get install fio
pip install paramiko
pip install texttable

yes | ssh-keygen -q -f ceph_test_rsa -N ''

nova keypair-add --pub-key ceph_test_rsa.pub ceph-test

