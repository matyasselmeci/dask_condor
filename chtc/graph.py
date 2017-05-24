#!/usr/bin/env python
from __future__ import print_function

import csv
import itertools
import time

from distributed import Client


START_TIMEOUT = 900  # 15 min
MAX_COLLECT_TIME = 86400  # 1 day


def running_task_list(cli):
    return list(itertools.chain.from_iterable(cli.processing().values()))


cli = Client('127.0.0.1:8786')

print("Waiting for tasks to start running")

timeout = time.time() + START_TIMEOUT

while not cli.ncores():
    time.sleep(5)
    if time.time() > timeout:
        raise Exception("workers never started")

print("First worker connected. Starting data collection.")

start_time = time.time()
end_time = time.time() + MAX_COLLECT_TIME

with open('graph.csv', 'wb') as outfile:
    writer = csv.writer(outfile)

    while cli.ncores() and time.time() < end_time:
        n_running_tasks = len(running_task_list(cli))
        n_cores = sum(cli.ncores().values())
        n_futures = len(cli.who_has().keys())

        row = [time.time() - start_time, n_cores, n_running_tasks, n_futures]
        print("{0:>6.0f}s {1:>5d} cores {2:>5d} tasks {3:>5d} futures".format(*row))
        writer.writerow(row)

        time.sleep(5)

print("Done with data collection.")
