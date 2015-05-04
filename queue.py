# -*- coding: utf-8 -*-

import json
import time
from keys import QueueKeys
from client import RedisClient


class Queue(object):

    def __init__(self, name):
        #init
        self.redis_client = RedisClient()
        self.name = name

        #add queue name in a redis set
        self.redis_client.redis.sadd(
            QueueKeys.queue_namespace_key(), name)

        self.input_queue_key = QueueKeys.input_queue_key(self.name)

        # ToDo...
        #startStatsWriter()

    def put(self, data):
        package = {
            'cteated_at': time.time(),
            'data': data,
            'queue': self.name,
            'acked': False,
        }
        str_package = json.dumps(package)

        #add message to input queue
        input_key = QueueKeys.input_queue_key(self.name)
        self.redis_client.redis.lpush(input_key, str_package)

        #incr input rate
        # input_rate_key = QueueKeys.input_queue_rate_key(self.name)
        # self.redis_client.redis.incr(input_rate_key, amount=1)


if __name__ == "__main__":
    q = Queue(name='test_queue')
    for i in xrange(1, 100000):
        print i
        q.put("test payload")