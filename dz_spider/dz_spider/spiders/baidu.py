# -*- coding: utf-8 -*-
import time
import json
import scrapy
import requests
import urllib.parse
from pipelines import MysqlPipline
from scrapy.log import logger


class BaiduSpider(scrapy.Spider):
    name = 'baidu'
    allowed_domains = ['www.baidu.com']
    start_urls = ['https://www.baidu.com/s?wd=2018%E5%B9%B46%E6%9C%881%E6%97%A5%E8%BE%BE%E5%B7%9E%E5%B8%82%E5%A5%BD%E4%B8%80%E6%96%B0%E5%A4%A7%E7%81%AB%E4%BA%8B%E4%BB%B6&oq=2018%E5%B9%B46%E6%9C%881%E6%97%A5%E8%BE%BE%E5%B7%9E%E5%B8%82%E5%A5%BD%E4%B8%80%E6%96%B0%E5%A4%A7%E7%81%AB%E4%BA%8B%E4%BB%B6&ie=utf-8&rsv_pq=de0d66320006de70&rsv_t=a244HdKKpXzA2TaA7xIlhfudcHE7OQ30N1QoR7twy3vznXDO%2BM4iUqnnapo']
    mysql = MysqlPipline()


    def start_requests(self):
        for url in self.start_urls:
            for page in range(5,70):
                yield scrapy.Request(url=f"{url}&pn={page}0")

    def parse(self, response):
        t1=time.time()
        html=scrapy.Selector(text=response.text)
        divs=html.css("#content_left  > div .f13 .c-tools::attr(data-tools)")
        for div in divs:
            data_str=div.extract()
            data_dict=json.loads(data_str)
            url=None
            try:
                url=requests.get(data_dict['url'],timeout=5).url
                schame = urllib.parse.urlparse(url).netloc
                sql = f"insert into seed(url,title,site_name,type) values('{url}','{data_dict['title']}','{schame}',1)"
                self.mysql.excute_sql(sql)
            except Exception as e:
                logger.error(f"requests.get(data_dict['url']).url ===>>> {str(e)}")
        t2=time.time()
        logger.info(f"执行===>>> {response.url} 花费时间{str(t2-t1)}")