# -*-coding:utf-8-*-
# 随机更换user agent
import random
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
import base64
import requests
import redis
import datetime
import time


class RotateUserAgentMiddleware(UserAgentMiddleware):
    def __init__(self, user_agent=''):
        self.user_agent = user_agent
        self._redis=redis.Redis(host="10.29.4.242",port=6379,db=0)
        # self.start_time=datetime.datetime.now()


    def get_proxy(self,name):
        key = self._redis.hgetall(name=name)
        rkey = random.choice(list(key.keys())) if key else None
        if isinstance(rkey, bytes):
            return rkey.decode('utf-8')
        else:
            return rkey

    def process_request(self, request, spider):
        ua = random.choice(self.user_agent_list)
        #ip = random.choice(self.ip_proxy)
        # ip = self.get_proxy("useful_proxy")
        if ua:
            request.headers.setdefault('User-Agent', ua)
            # request.headers.setdefault('')
            #request.meta['proxy'] = 'http://{0}'.format(ip)

            #response=requests.get("http://10.29.4.242:5010/get/")
            #print('http://{0}'.format(response.text))
            #request.meta['proxy'] = 'http://{0}'.format(response.text)
            # request.meta['proxy'] = "127.0.0.1:8888"
            # proxy_user_pass = 'XXXXXXXXXXXXXXX:KKKKKKKKKKKKKKKK'
            # encoded_user_pass = base64.b64encode(proxy_user_pass.encode(encoding='utf-8'))
            # request.headers['Proxy-Authorization'] = 'Basic ' + str(encoded_user_pass)

    ip_proxy = ['101.50.1.2:80', '54.36.1.22:3128', '178.238.228.187:9090', '149.56.108.133:3128', '190.2.137.31:1080', '13.125.162.226:3128', '128.199.182.128:3128', '122.216.120.254:80', '157.55.233.183:80', '85.10.247.140:1080', '90.84.242.77:3128', '159.65.156.178:3128', '54.38.100.98:1080', '119.28.221.28:8088', '139.224.24.26:8888', '190.2.137.9:1080', '178.32.181.66:3128', '47.88.35.91:3128', '103.78.213.147:80', '59.44.164.34:3128', '190.2.137.15:1080', '54.36.31.203:3128', '142.44.198.187:3128', '122.114.31.177:808', '66.195.76.86:8080', '122.216.120.244:80', '212.237.34.18:8888', '134.119.205.147:1080', '159.89.201.219:3128', '50.28.48.83:8080', '211.159.219.158:80', '124.51.247.48:3128', '35.162.122.16:8888', '217.182.242.64:3128', '139.59.21.37:3128', '47.89.23.174:8080', '200.16.208.187:8080', '5.135.74.36:1080', '117.242.145.103:8080', '61.5.207.102:80', '61.135.217.7:80', '71.13.112.152:3128', '5.135.74.37:1080', '211.159.177.212:3128', '210.5.149.43:8090', '122.72.18.35:80', '212.237.51.54:8888', '61.136.163.245:8107', '124.193.37.5:8888', '120.78.182.79:3128', '180.173.67.197:9797', '171.97.67.88:3128', '145.239.185.127:1080', '167.99.70.26:8080', '159.65.141.81:3128', '180.235.42.148:8080', '67.205.159.46:3128', '121.8.98.198:80', '151.80.140.233:54566', '139.59.224.113:8080', '47.91.165.126:80', '5.9.78.89:3128', '142.44.202.122:3128', '35.198.103.196:3128', '39.137.47.11:80', '142.44.197.15:3128', '190.2.137.38:1080', '122.216.120.251:80', '159.65.139.226:3128', '116.11.254.37:80', '36.80.123.114:3128', '194.67.220.181:3128', '217.182.216.236:3128', '190.2.137.47:1080', '163.172.217.103:3128', '145.239.185.122:1080', '212.237.37.152:8888', '219.135.164.245:3128', '119.28.26.57:3128', '120.77.254.116:3128', '60.207.106.140:3128', '14.139.189.216:3128', '212.126.117.158:80', '120.26.160.183:8090', '142.44.198.121:3128', '218.50.2.102:8080', '183.179.199.225:8080', '116.58.227.143:3128', '144.202.70.37:3128', '119.28.112.130:3128', '45.63.95.172:3128', '167.99.87.147:8080', '202.175.61.162:8080', '200.63.129.131:80', '194.182.74.203:3128', '77.244.21.75:3128', '118.212.137.135:31288', '145.239.185.121:1080', '190.2.137.45:1080', '5.167.54.154:8080', '50.233.137.38:80', '112.21.164.58:1080', '45.76.56.140:3128', '35.200.194.218:3128', '159.65.142.92:3128', '37.204.219.50:8081', '113.214.13.1:8000', '47.90.72.227:8088', '114.130.42.20:80', '119.28.152.208:80', '167.99.78.239:8080', '144.202.70.81:3128', '151.80.9.177:3128', '151.106.10.230:1080', '104.155.53.214:3128', '123.57.133.142:3128', '151.106.5.26:1080', '5.9.78.28:3128', '47.75.56.36:8118', '66.70.147.195:3128', '114.232.171.58:48354', '122.72.18.34:80', '5.135.74.32:1080', '114.130.42.20:3128']

    user_agent_list = [ \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1" \
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]