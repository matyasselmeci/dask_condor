#!/usr/bin/env python
from __future__ import print_function

import distributed
import time

def sleepsort(arg):
    time.sleep(arg/10.0)
    print(arg)
    return arg

cluster = distributed.LocalCluster(diagnostics_port=None)
client = distributed.Client(cluster)
A = client.map(ord, 'HELLO')
B = client.map(sleepsort, A, pure=False)
C = client.map(chr, B)
print(client.gather(C))
try:
    client.shutdown()
except distributed.core.CommClosedError:
    pass

