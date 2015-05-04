# -*- coding: utf-8 -*-

import redis
import threading
import logging
from redis.connection import ConnectionPool

redis_settings = {
    "servers": "localhost",
    "port": 6379,
    "db": 11
}

_pool0 = ConnectionPool(**redis_settings)


class RedisClient(object):

    instance = None
    locker = threading.Lock()

    def __init__(self):
        """ intialize the client of redis  include port db and servers """
        try:
            self.servers = redis_settings["servers"]
            self.port = redis_settings["port"]
            self.db = redis_settings["db"]
            self.redis = redis.Redis(
                connection_pool=_pool0
            )
        except Exception, e:
            logging.error(e)

    @classmethod
    def get_instance(cls):
        """
        get the instance of RedisClient
        return: the redis client
        """
        cls.locker.acquire()
        try:
            if not cls.instance:
                cls.instance = cls()
            return cls.instance
        finally:
            cls.locker.release()

    def reconnect(self):
        """
        if the connetion is disconnet  then connect again
        """
        try:
            self.redis = redis.Redis(self.servers, self.port, self.db)
        except Exception, e:
            print e