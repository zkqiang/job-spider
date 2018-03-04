#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from threading import Thread
from multiprocessing import Process
from .spider import SpiderMeta
import queue
import logging
import csv
import time
import re


class SpiderProcess(Process):
    """爬虫进程"""

    def __init__(self, data_queue, job, city):
        Process.__init__(self)
        self.data_queue = data_queue
        self.job = job
        self.city = city
        self.logger = logging.getLogger('root')

    def set_logging(self):
        """设置日志格式"""
        self.logger = logger = logging.getLogger('root')
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def iter_spider(self, spider):
        """对爬虫类的`crawl`方法进行迭代，数据送入队列传给另一进程"""
        setattr(spider, 'job', self.job)
        setattr(spider, 'city', self.city)
        generator = spider.crawl()
        if generator:
            for result in spider.crawl():
                if not result:
                    continue
                # 去除内容里的空格换行
                for key in result.keys():
                    result[key] = re.sub(r'\s+', '', result[key])
                self.data_queue.put(result)
                self.logger.debug('%s %s %50s...(省略)' % (
                    result.get('title'), result.get('url'), result.get('description')))
        self.logger.info('%s 爬虫已结束' % spider.__class__.__name__)

    def run(self):
        """对每个爬虫类启动单独线程"""
        self.set_logging()
        spiders = [cls() for cls in SpiderMeta.spiders]
        spider_count = len(spiders)
        threads = []
        for i in range(spider_count):
            t = Thread(target=self.iter_spider, args=(spiders[i], ))
            t.setDaemon(True)
            t.start()
            threads.append(t)
        while True:
            time.sleep(1)


class WriterProcess(Process):
    """写数据进程"""

    def __init__(self, data_queue):
        Process.__init__(self)
        self.data_queue = data_queue

    def run(self):
        """以当前时间创建 csv 文件，并从队列中获取数据写入"""
        csv_name = datetime.now().strftime('%Y-%m-%d %H-%M-%S') + '.csv'
        with open(csv_name, 'w', encoding='utf_8_sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['标题', '公司', '薪水', '经验',
                             '学历', '链接', '描述'])
            while True:
                try:
                    result = self.data_queue.get(timeout=90)
                    if result:
                        row = [
                            result.get('title'), result.get('company'),
                            result.get('salary'), result.get('experience'),
                            result.get('education'), result.get('url'),
                            result.get('description')
                        ]
                        writer.writerow(row)
                except queue.Empty:
                    f.close()
