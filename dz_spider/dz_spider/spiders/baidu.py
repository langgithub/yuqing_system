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
    start_urls = ['https://www.baidu.com/s?wd=2018%E5%B9%B410%E6%9C%887%E6%97%A5%E8%BE%BE%E5%B7%9D%E5%8C%BA%E5%8D%97%E5%A4%96%E6%B5%8E%E6%B0%91%E5%8C%BB%E9%99%A2%E9%97%A8%E5%8F%A3%E7%AA%81%E7%84%B6%E5%A1%8C%E9%99%B7%E4%BA%8B%E4%BB%B6&oq=2018%E5%B9%B410%E6%9C%887%E6%97%A5%E8%BE%BE%E5%B7%9D%E5%8C%BA%E5%8D%97%E5%A4%96%E6%B5%8E%E6%B0%91%E5%8C%BB%E9%99%A2%E9%97%A8%E5%8F%A3%E7%AA%81%E7%84%B6%E5%A1%8C%E9%99%B7%E4%BA%8B%E4%BB%B6&ie=utf-8&rsv_idx=1&rsv_pq=da4e0d0600051217&rsv_t=0bdcDWC5g2e2v0%2FFpxTTPC6IQO3RvUQxRCleqWWkBvdvuCKNo6MtAkayKAM']
    mysql = MysqlPipline()


    def start_requests(self):
        for url in self.start_urls:
            for page in range(50):
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
                url=requests.get(data_dict['url']).url
                schame = urllib.parse.urlparse(url).netloc
                sql = f"insert into seed(url,title,site_name,type) values('{url}','{data_dict['title']}','{schame}',1)"
                self.mysql.excute_sql(sql)
            except Exception as e:
                logger.error(f"requests.get(data_dict['url']).url ===>>> {str(e)}")
        t2=time.time()
        logger.info(f"执行===>>> {response.url} 花费时间{str(t2-t1)}")