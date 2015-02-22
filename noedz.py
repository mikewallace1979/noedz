#!/usr/bin/env python

import random
import time

from collections import deque
from multiprocessing import Process, Queue, Manager

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

def init(num_workers=3, debug=False):
    worker_procs = deque()
    broker_proc, broker_q = broker_init()
    debug_queues = debug and {}
    m = Manager()
    for pid in range(0, num_workers):
        inbox = m.Queue()
        broker_q.put(('register', pid, inbox))
        if debug:
            debug_queues[pid] = Queue()
        worker_proc = Process(
            target=worker,
            args=(pid, inbox),
            kwargs={"debug": debug and debug_queues[pid]}
        )
        worker_proc.start()
        worker_procs.append(worker_proc)
    return worker_procs, broker_proc, broker_q, debug_queues

if __name__ == '__main__':
    random.seed(time.time())
    procs, broker_proc, broker_q, debug_queues = init(num_workers=3)
    for p in procs:
        p.join()
