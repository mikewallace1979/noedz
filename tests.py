import unittest

from collections import deque
from multiprocessing import Process, Queue, Manager

from Queue import Empty

from noedz import worker, broker, broker_init, init

_WORKERS = 3

class TestNoedz(unittest.TestCase):
    def setUp(self):
        self.procs, self.broker_proc, self.send, self.broker_register, self.debug_queues = init(
            num_workers=_WORKERS,
            debug=True
        )
        # Drain debug queues of any messages from init
        for q in self.debug_queues.values():
            try:
                while True:
                    q.get(timeout=0.1)
            except Empty:
                pass

    def tearDown(self):
        for p in self.procs:
            p.terminate()
        self.broker_proc.terminate()

    def testMessage(self):
        target_worker = 0
        sender_pid = -1
        test_msg = 'Message from {0}'.format(sender_pid)
        self.send(sender_pid, target_worker, test_msg)
        msg = self.debug_queues[target_worker].get(timeout=5)
        self.assertEquals(msg, '{0} received message from {1}: {2}'.format(
            target_worker,
            sender_pid,
            test_msg
        ))

    def testSendList(self):
        target_worker = 0
        sender_pid = -1
        test_msg = ['foo', 'bar', {'baz': 'quux'}]
        self.send(sender_pid, target_worker, test_msg)
        msg = self.debug_queues[target_worker].get(timeout=5)
        self.assertEquals(msg, '{0} received message from {1}: {2}'.format(
            target_worker,
            sender_pid,
            test_msg
        ))

    def testSendPeer(self):
        sender_pid = -1
        src_worker = 0
        dst_worker = 1
        peer_msg = 'Message from {0}'.format(src_worker)
        test_msg = ('send', dst_worker, peer_msg)
        self.send(sender_pid, src_worker, test_msg)
        msg = self.debug_queues[dst_worker].get(timeout=5)
        self.assertEqual(msg, "{0} received message from {1}: ('{2}',)".format(
            dst_worker,
            src_worker,
            peer_msg
        ))

    def testPing(self):
        sender_pid = -1
        src_worker = 0
        dst_worker = 1
        test_msg = ('send', dst_worker, 'ping')
        self.send(sender_pid, src_worker, test_msg)
        self.assertEqual(self.debug_queues[src_worker].get(timeout=5),
            '{0} received message from {1}: {2}'.format(
                src_worker,
                sender_pid,
                test_msg
            )
        )
        self.assertEqual(self.debug_queues[src_worker].get(timeout=5),
            '{0} received message from {1}: {2}'.format(
                src_worker,
                dst_worker,
                'pong'
            )
        )

    def testPutGetSingleNode(self):
        sender_pid = -1
        inbox = self.broker_register(sender_pid)
        tgt_worker = 0
        test_key = 'kitteh'
        test_value = 'ohai'
        put_msg = ('put', test_key, test_value)
        self.send(sender_pid, tgt_worker, put_msg)
        self.assertEquals(inbox.get(timeout=5), (0, 'ok'))
        get_msg = ('get', test_key)
        self.send(sender_pid, tgt_worker, get_msg)
        self.assertEquals(inbox.get(timeout=5), (0, test_value))

    def testPutGetMultinode(self):
        sender_pid = -1
        inbox = self.broker_register(sender_pid)
        put_worker = 0
        get_worker = 1
        test_key = 'kitteh'
        test_value = 'ohai'
        put_msg = ('cput', test_key, test_value)
        self.send(sender_pid, put_worker, put_msg)
        self.assertEquals(inbox.get(timeout=5), (put_worker, 'ok'))
        get_msg = ('get', test_key)
        self.send(sender_pid, get_worker, get_msg)
        self.assertEquals(inbox.get(timeout=5), (get_worker, test_value))

    def testGetNotFound(self):
        sender_pid = -1
        inbox = self.broker_register(sender_pid)
        tgt_worker = 0
        test_key = 'kitteh'
        get_msg = ('get', test_key)
        self.send(sender_pid, tgt_worker, get_msg)
        self.assertEquals(inbox.get(timeout=5), (0, 'error'))
