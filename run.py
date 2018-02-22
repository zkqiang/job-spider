#!/usr/bin/env python
# -*- coding: utf-8 -*-

from job_spider.process import SpiderProcess, WriterProcess
from job_spider.optarg import Arg
from multiprocessing import Queue


def main():
    arg = Arg()
    job = arg.get_arg('-j')
    city = arg.get_arg('-c')
    queue = Queue()
    p1 = SpiderProcess(queue, job, city)
    p2 = WriterProcess(queue)
    p1.start()
    p2.start()
    p1.join()
    p2.join()


if __name__ == '__main__':
    main()
