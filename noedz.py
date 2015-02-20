#!/usr/bin/env python

import random
import time

from collections import deque
from multiprocessing import Process, Queue

from Queue import Empty

WORKERS = 3

def send(msg, src_pid, dst):
    dst.put((src_pid, msg), False)

def receive(q):
    try:
        return q.get(False)
    except Empty:
        return None, None

def maybe_receive_msg(pid, q, debug=None):
    sender_pid, msg = receive(q)
    if msg:
        output = '{0} received message from {1}: {2}'.format(pid, sender_pid, msg)
        print output
        if debug:
            debug.put(output)

def worker(pid, queues, debug=None):
    print 'Ohai I am worker {0}'.format(pid)
    while True:
        maybe_receive_msg(pid, queues[pid], debug)

if __name__ == '__main__':
    random.seed(time.time())
    procs = deque()
    queues = {}
    for pid in range(0, WORKERS):
        queues[pid] = Queue()
    for pid in range(0, WORKERS):
        p = Process(target=worker, args=(pid, queues))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()
