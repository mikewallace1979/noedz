#!/usr/bin/env python

import cmd
import random
import shlex
import time

from collections import deque
from functools import partial
from multiprocessing import Process, Queue, Manager

from Queue import Empty

# Workaround for tab completion on Mac OS X
import readline
readline.parse_and_bind('bind ^I rl_complete')
# End workaround

WORKERS = 3
TIMEOUT = 5

def broker_enqueue(broker_q, msg):
    broker_q.put(msg)

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
            continue
        if msg[0] == 'register':
            pid = msg[1]
            queue = msg[2]
            _broker_register(queues, pid, queue)
        if msg[0] == 'send':
            src = msg[1]
            dst = msg[2]
            payload = msg[3]
            _broker_send(queues, src, dst, payload)

def _worker_send(broker_q, src, dst, msg):
    broker_q.put(('send', src, dst, msg), False)

def receive(q):
    try:
        return q.get(timeout=TIMEOUT)
    except Empty:
        return None, None

def maybe_receive_msg(pid, q, debug=None):
    sender_pid, msg = receive(q)
    if msg:
        output = '{0} received message from {1}: {2}'.format(pid, sender_pid, msg)
        if debug:
            debug.put(output)
        return sender_pid, msg
    else:
        return None, None

def worker(pid, queue, broker_q, debug=None):
    while True:
        src_pid, msg = maybe_receive_msg(pid, queue, debug)
        if not msg:
            continue
        elif msg[0] == 'send':
            dst_pid = msg[1]
            peer_msg = msg[2]
            _worker_send(broker_q, pid, dst_pid, peer_msg)

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
            args=(pid, inbox, broker_q),
            kwargs={"debug": debug and debug_queues[pid]}
        )
        worker_proc.start()
        worker_procs.append(worker_proc)
    enqueue_fun = partial(broker_enqueue, broker_q)
    return worker_procs, broker_proc, enqueue_fun, debug_queues

class NoedzShell(cmd.Cmd):
    file = None

    def __init__(self, worker_procs, broker_proc, broker_enqueue, debug_queues):
        cmd.Cmd.__init__(self, completekey='TAB')
        self.pid = -1
        self.broker_proc = broker_proc
        self.worker_procs = worker_procs
        self.broker_enqueue = broker_enqueue
        self.debug_queues = debug_queues
        self.prompt = 'noedz > '

    def do_exit(self, arg):
        for p in self.worker_procs:
            p.terminate()
        self.broker_proc.terminate()
        exit(0)

    def do_dump_debug(self, arg):
        pid = int(arg)
        try:
            print self.debug_queues[pid].get(False)
        except Empty:
            print 'Nothing in the queue'

    def do_send(self, arg):
        args = shlex.split(arg)
        dst = int(args[0])
        msg = self._parse_args(args[1:])
        self.broker_enqueue(('send', self.pid, dst, msg))

    def _parse_fun(self, arg):
        try:
            return int(arg)
        except ValueError:
            return arg

    def _parse_args(self, args):
        return [self._parse_fun(arg) for arg in args]

if __name__ == '__main__':
    random.seed(time.time())
    worker_procs, broker_proc, broker_enqueue, debug_queues = init(num_workers=3, debug=True)
    NoedzShell(worker_procs, broker_proc, broker_enqueue, debug_queues).cmdloop()
