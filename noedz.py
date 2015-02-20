#!/usr/bin/env python

import random
import time

from collections import deque
from multiprocessing import Process, Queue

from Queue import Empty

WORKERS = 3
MAGIC = 100000

def send(msg, src_pid, dst):
    dst.put((src_pid, msg), False)

def maybe_send_msg(pid, queues):
    if random.randint(1, MAGIC) == 1:
        dst_pid, dst = random.choice(queues.items())
        print '{0} sending message to {1}'.format(pid, dst_pid)
        send('Message from {0}'.format(pid), pid, dst)

def receive(q):
    try:
        return q.get(False)
    except Empty:
        return None, None

def maybe_receive_msg(pid, q):
    sender_pid, msg = receive(q)
    if msg:
        print '{0} receieved message from {1}: {2}'.format(pid, sender_pid, msg)

def maybe_broadcast(pid, queues):
    if random.randint(1, MAGIC) == 1:
        print '{0} broadcasting'.format(pid)
        for q in queues.values():
            send('Broadcast from {0}'.format(pid), pid, q)

def worker(pid, queues):
    print 'Ohai I am worker {0}'.format(pid)
    while True:
        maybe_send_msg(pid, queues)
        maybe_broadcast(pid, queues)
        maybe_receive_msg(pid, queues[pid])

def message_eater(queues):
    while True:
        victim_pid, victim = random.choice(queues.items())
        sender_pid, msg = receive(victim)
        if msg:
            print 'Message from {0} to {1} containing "{2}" has been EATEN'.format(sender_pid, victim_pid, msg)

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
    message_eater = Process(target=message_eater, args=(queues, ))
    message_eater.start()
    for p in procs:
        p.join()
