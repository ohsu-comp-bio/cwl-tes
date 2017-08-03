from __future__ import absolute_import, print_function, unicode_literals

import threading


class PollThread(threading.Thread):

    def __init__(self, operation, poll_interval=1, poll_retries=10):
        super(PollThread, self).__init__()
        self.daemon = True
        self.operation = operation
        self.id = operation.id
        self.poll_interval = poll_interval
        self.poll_retries = poll_retries

    def poll(self):
        raise Exception("PollThread.poll() not implemented")

    def is_done(self, operation):
        raise Exception("PollThread.is_done(operation) not implemented")

    def complete(self, operation):
        raise Exception("PollThread.complete(operation) not implemented")

    def run(self):
        raise Exception("PollThread.run() not implemented")
