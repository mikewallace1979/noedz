import unittest

from collections import deque
from multiprocessing import Process, Queue

from noedz import send, worker

_WORKERS = 3

class TestNoedz(unittest.TestCase):
    def setUp(self):
        self.procs = deque()
        self.queues = {}
        self.debug_queues = {}
        for pid in range(0, _WORKERS):
            self.queues[pid] = Queue()
            self.debug_queues[pid] = Queue()
        for pid in range(0, _WORKERS):
            p = Process(
                target=worker,
                args=(pid, self.queues),
                kwargs={"debug": self.debug_queues[pid]}
            )
            p.start()
            self.procs.append(p)

    def tearDown(self):
        for p in self.procs:
            p.terminate()

    def testMessage(self):
        target_worker = 0
        sender_pid = -1
        test_msg = 'Message from {0}'.format(sender_pid)
        send(test_msg, sender_pid, self.queues[target_worker])
        msg = self.debug_queues[target_worker].get(timeout=5)
        assert msg == '{0} receieved message from {1}: {2}'.format(
            target_worker,
            sender_pid,
            test_msg
        )

    def testSendList(self):
        target_worker = 0
        sender_pid = -1
        test_msg = ['foo', 'bar', {'baz': 'quux'}]
        send(test_msg, sender_pid, self.queues[target_worker])
        msg = self.debug_queues[target_worker].get(timeout=5)
        self.assertEquals(msg, '{0} receieved message from {1}: {2}'.format(
            target_worker,
            sender_pid,
            test_msg
        ))
