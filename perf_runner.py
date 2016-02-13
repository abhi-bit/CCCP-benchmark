#!/usr/bin/env python

import datetime
import multiprocessing
from rest import Rest
import sys
import yaml

CONFIG = yaml.load(file('config.yaml', 'rb'))

CALL_COUNT_PER_CHILD = int(sys.argv[1])
PARALLELISM = int(sys.argv[2])


def do_config_call(ip, admin_port, mcd_port):
    rest = Rest(ip, admin_port, mcd_port)
    dcp_client = rest.vbDCPClient(0)
    for i in xrange(0, CALL_COUNT_PER_CHILD):
        dcp_client.get_cluster_config(0)


def main():
    cbconf = CONFIG['couchbase']
    ip = cbconf['ip']
    admin_port = int(cbconf['admin_port'])
    mcd_port = int(cbconf['mcd_port'])

    jobs = []
    pool_size = multiprocessing.cpu_count() * PARALLELISM
    a = datetime.datetime.now().replace(microsecond=0)
    for i in xrange(pool_size):
        p = multiprocessing.Process(target=do_config_call, args=(ip,
                                                                 admin_port,
                                                                 mcd_port))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

    b = datetime.datetime.now().replace(microsecond=0)
    print "Fired %d CCCP calls" % (CALL_COUNT_PER_CHILD * pool_size)
    print "Time elapsed: %s sec" % (b-a)

if __name__ == "__main__":
    main()
