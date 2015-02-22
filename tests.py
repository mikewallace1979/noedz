import unittest

from collections import deque
from multiprocessing import Process, Queue, Manager

from noedz import send, worker, broker, broker_init, init

_WORKERS = 3

class TestNoedz(unittest.TestCase):
    def setUp(self):
        self.procs, self.broker_proc, self.broker_q, self.debug_queues = init(
            num_workers=_WORKERS,
            debug=True
        )

    def tearDown(self):
        for p in self.procs:
            p.terminate()
        self.broker_proc.terminate()

    def testMessage(self):
        target_worker = 0
        sender_pid = -1
        test_msg = 'Message from {0}'.format(sender_pid)
        self.broker_q.put(('send', sender_pid, target_worker, test_msg))
        msg = self.debug_queues[target_worker].get(timeout=5)
        assert msg == '{0} received message from {1}: {2}'.format(
            target_worker,
            sender_pid,
            test_msg
        )

    def testSendList(self):
        target_worker = 0
        sender_pid = -1
        test_msg = ['foo', 'bar', {'baz': 'quux'}]
        self.broker_q.put(('send', sender_pid, target_worker, test_msg))
        msg = self.debug_queues[target_worker].get(timeout=5)
        self.assertEquals(msg, '{0} received message from {1}: {2}'.format(
            target_worker,
            sender_pid,
            test_msg
        ))
