# -*- coding: utf-8 -*-


class QueueKeys(object):

    @staticmethod
    def queue_namespace_key():
        return 'queue_namespace'

    @staticmethod
    def input_queue_key(queue_name):
        return 'input_queue_key:%s' % queue_name

    @staticmethod
    def input_queue_rate_key(queue_name):
        return 'input_queue_rate_key:%s' % queue_name

    @staticmethod
    def working_queue_key(queue_name, customer_name):
        return 'working_queue_key_queue:%s_customer:%s' % (
            queue_name, customer_name
        )

    @staticmethod
    def working_queue_rate_key(queue_name,customer_name):
        return 'working_queue_rate_key_queue:%s_customer:%s' % (
            queue_name, customer_name
        )

    @staticmethod
    def failed_queue_key(queue_name, customer_name):
        return 'failed_queue_key:%s_customer:%s' % (
            queue_name, customer_name
        )


if __name__ == "__main__":
    print QueueKeys.input_queue_key("test")
    print QueueKeys.working_queue_key("test")
    print QueueKeys.failed_queue_key("test")