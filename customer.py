# -*- coding: utf-8 -*-

import logging
from keys import QueueKeys
from queue import Queue


class Customer(object):

    HAS_UNACK = -1

    def __init__(self, customer_name, queue_name):
        self.mq = Queue(name=queue_name)
        self.name = customer_name
        #keys
        self.working_queue_key = QueueKeys.working_queue_key(
            self.mq.name, self.name)
        self.fail_queue_key = QueueKeys.failed_queue_key(
            self.mq.name, self.name)

    def get(self):
        #if has unack package
        if self.has_unack_package():
            logging.error(
                "cunstomer %s has unack package",
                self.name
            )
            return Customer.HAS_UNACK

        return self.unsafe_get()

    def unsafe_get(self):
        #pop data from input queue and push to working queue
        package = self.mq.redis_client.redis.brpoplpush(
            self.mq.input_queue_key, self.working_queue_key
        )
        return package

    def ack_package(self):
        try:
            self.mq.redis_client.redis.rpop(
                self.working_queue_key
            )
        except Exception, e:
            logging.error(
                "ack package error queue: %s customer :%s error %s" %
                (self.mq.name, self.name, e)
            )

    def fail_package(self):
        try:
            self.mq.redis_client.redis.rpoplpush(
                self.working_queue_key,
                self.fail_queue_key,
            )
        except Exception,e:
            logging.error(
                "fail package error queue: %s customer :%s error %s" %
                (self.mq.name, self.name, e)
            )

    def requeue_package(self):
        try:
            self.mq.redis_client.redis.rpoplpush(
                self.working_queue_key,
                self.mq.input_queue_key,
            )
        except Exception,e:
            logging.error(
                "requeue package error queue: %s customer :%s error %s" %
                (self.mq.name, self.name, e)
            )

    def get_unacked(self):
        unack_package = self.mq.redis_client.redis.lindex(
            self.working_queue_key, -1
        )

        return unack_package

    def has_unacked(self):
        working_lenth = self.mq.redis_client.redis.llen(
            self.working_queue_key
        )
        if working_lenth == 0:
            return False
        else:
            return True

    def reset_working(self):
        self.mq.redis_client.redis.delete(
            self.working_queue_key
        )

    def requeue_working(self):
        working_lenth = self.mq.redis_client.redis.llen(
            self.working_queue_key
        )

        for i in xrange(0, working_lenth):
            self.mq.redis_client.redis.rpoplpush(
                self.working_queue_key, self.mq.input_queue_key,
            )


if __name__ == "__main__":
    import os
    pid = os.getpid()
    print pid
    i = 0
    while True:
        c = Customer(
            'test_customer_%s' % pid,
            'test_queue'
        )
        data = c.get()
        print data
        if data == Customer.HAS_UNACK:
            c.requeue()

        c.ack_package()
        i += 1
        print i