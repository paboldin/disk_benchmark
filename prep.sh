#!/bin/bash
set -x
set -e
set -o pipefail

apt-get install iozone3
apt-get install fio
apt-get install python-paramiko
apt-get install python-texttable

yes | ssh-keygen -q -f ceph_test_rsa -N ''
nova keypair-add --pub-key ceph_test_rsa.pub ceph_test
