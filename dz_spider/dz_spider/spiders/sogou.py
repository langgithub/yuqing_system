# -*- coding: utf-8 -*-
import time
import json
import scrapy
import requests
import urllib.parse
from dz_spider.pipelines import MysqlPipline
from scrapy.log import logger


class SogouSpider(scrapy.Spider):
    name = 'sogou'
    allowed_domains = ['www.sogou.com']
    start_urls = ['https://www.sogou.com/tx?query=2018%E5%B9%B48%E6%9C%88%E8%BE%BE%E5%B7%9E%E5%87%BA%E7%A7%9F%E8%BD%A6%E7%BD%A2%E5%B7%A5%E4%BA%8B%E4%BB%B6&hdq=sogou-wsse-3f7bcd0b3ea82268&duppid=1&cid=&s_from=result_up&sut=1606&sst0=1565226159021&lkt=0,0,0&sugsuv=006E51C00177C3995CB434B8FE950363&sugtime=1565226159021&ie=utf8&w=01029901&dr=1']
    mysql = MysqlPipline()


    def start_requests(self):
        for url in self.start_urls:
            for page in range(0,3):
                yield scrapy.Request(url=f"{url}&page={page}")

    def parse(self, response):
        t1=time.time()
        html=scrapy.Selector(text=response.text)
        divs=html.css("div.results > div")
        for div in divs:
            vrwrap=div.css("div.vrwrap")
            if len(vrwrap)==0:
                title = "".join(div.css("div.rb h3 a::text").extract())
                url = "https://www.sogou.com" + div.css("div.rb h3 a::attr(href)").extract()[0]
            else:
                title="".join(div.css("div.vrwrap h3 a::text").extract())
                url = "https://www.sogou.com"+div.css("div.vrwrap h3 a::attr(href)").extract()[0]
            try:
                _html=scrapy.Selector(text=requests.get(url,verify=False).text)
                url = _html.re("window.location.replace\(\"(.*?)\"\)")[0]
                schame = urllib.parse.urlparse(url).netloc
                sql = f"insert into seed(url,title,site_name,type) values('{url}','{title}','{schame}',1)"
                self.mysql.excute_sql(sql)
            except Exception as e:
                logger.error(f"requests.get(data_dict['url']).url ===>>> {str(e)}")
            t2=time.time()
            logger.info(f"执行===>>> {response.url} 花费时间{str(t2-t1)}")
