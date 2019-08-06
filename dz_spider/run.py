# encoding: utf-8
"""
--------------------------------------
@describe 
@version: 1.0
@project: yuqing_system
@file: run.py
@author: yuanlang 
@time: 2019-07-26 17:12
---------------------------------------
"""

from scrapy import cmdline
# cmdline.execute(['scrapy', 'crawl', 'baidu'])
cmdline.execute(['scrapy', 'crawl', 'toutiao'])
# cmdline.execute(['scrapy', 'crawl', 'sogou'])


