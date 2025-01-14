import json

import redis


class RedisQueue:
    """A queeue made with Redis list allows FIFO features"""

    def __init__(self, qkey):
        self._queue = redis.StrictRedis(
            password="stevedang", port=6379, host="localhost", db=0
        )
        self.key = qkey

    def size(self):
        """Return approx size of the queue"""
        return self._queue.llen(self.key)

    def empty(self):
        """Return True if queue is empty, otherwise False"""
        return self.size() == 0

    def put(self, item):
        """Put an item at the tail the queue"""
        self._queue.rpush(self.key, json.dumps(item))

    def pop(self, block=True, timeout=None):
        """Pop an item from the queue. If block is True,
        and timeout is None, block until there's an item.
        """
        if block:
            item = self._queue.blpop(self.key, timeout=timeout)
        else:
            item = self._queue.lpop(self.key)
        if item:
            item = item[1]
            item = json.loads(item)  # Convert the item back to a dictionary
        return item
