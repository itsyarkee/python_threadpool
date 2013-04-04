#! /usr/bin/env python
#-*-coding:utf-8-*-

"""
A threadpool implemented by Python
author:  iyarkee@gmail.com
version: 1.0 , 2013-02-08
"""

from threading import Thread
import Queue
import logging
import time


class Worker(Thread):
    def __init__(self, work_q, res_q, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        self.daemon = True
        self.work_q = work_q
        self.res_q = res_q

    def run(self):
        while True:
            try:
                callback, args, kwargs = self.work_q.get()
                if callback is None:
                    break
                res = callback(*args, **kwargs)
                self.res_q.put(res)
            except Exception as e:
                logging.exception('')


class ThreadPool(object):
    def __init__(self, num_threads):
        self.work_q = Queue.Queue()
        self.res_q = Queue.Queue()
        self.workers = {}
        for i in range(num_threads):
            worker = Worker(self.work_q, self.res_q)
            self.workers[i] = worker

    def start(self):
        for i in self.workers:
            self.workers[i].start()

    def add_task(self, callback, *args, **kwargs):
        self.work_q.put((callback, args, kwargs))

    def get_result(self):
        return self.res_q

    def wait_for_complete(self):
        while True:
            if not self.work_q.empty():
                time.sleep(1)
            else:
                break

    def stop(self):
        for i in self.workers:
            self.work_q.put((None, None, None))
        for i in self.workers:
            self.workers[i].join()
        del self.workers


def test():
    def task(my_id, msg):
        return "id %d: %s" % (my_id, msg.upper())

    mypool = ThreadPool(10)
    for i in range(300):
        mypool.add_task(task, my_id = i, msg = "hello %d" %i)
    mypool.start()
    mypool.wait_for_complete()

    while not mypool.res_q.empty():
        print mypool.res_q.get()

    mypool.stop()


if __name__ == '__main__':
    test()
