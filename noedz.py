#!/usr/bin/env python

import random
import time

from collections import deque
from multiprocessing import Process, Queue

from Queue import Empty

WORKERS = 3

def broker_init():
    q = Queue()
    p = Process(target=broker, args=((q,)))
    p.start()
    return p, q

def _broker_register(queues, pid, queue):
    queues[pid] = queue

def _broker_send(queues, src, dst, msg):
    queues[dst].put((src, msg), False)

def broker(inbox):
    queues = {}
    while True:
        try:
            msg = inbox.get(timeout=1)
        except Empty:
            pass
        if msg[0] == 'register':
            pid = msg[1]
            queue = msg[2]
            _broker_register(queues, pid, queue)
        if msg[0] == 'send':
            src = msg[1]
            dst = msg[2]
            payload = msg[3]
            _broker_send(queues, src, dst, payload)

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

def worker(pid, queue, debug=None):
    print 'Ohai I am worker {0}'.format(pid)
    while True:
        maybe_receive_msg(pid, queue, debug)

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
