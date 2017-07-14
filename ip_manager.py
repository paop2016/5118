# -*- coding:utf-8 -*-
import sys,re
reload(sys)
sys.setdefaultencoding( "utf-8" )

import requests,json,time,random
from threading import Timer
from lxml import etree
pool={}

class IpManager():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'account.5118.com',
        'Referer': 'http://www.5118.com/',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }
    def __init__(self):
        pass
    def get_ip(self,trytimes=5):
        print '尝试切换ip'
        r=requests.get('http://www.xdaili.cn/ipagent//privateProxy/getDynamicIP/DD20177142661u9xze3/74ac6f0850c811e79d9b7cd30abda612?returnType=2')
        ipdata=json.loads(r.text)['RESULT']
        host = ipdata['wanIp']
        port = ipdata['proxyport']
        ip = 'http://' + host + ':' + str(port)
        print ip
        ip= {'http':ip}
        try:
            r=requests.get('https://www.baidu.com/', proxies=ip,timeout=5)
            r.encoding='utf-8'
        except Exception as e:
            print '访问百度超时'
        else:
            if '百度' in r.text:
                print 'ip可用'
                s=self.login(ip)
                if s:
                    if self.ip_test(ip,s):
                        print '返回可用ip:%s'%str(ip)
                        return ip,s
        if trytimes>0:
            print '等待重新尝试切换ip'
            time.sleep(20)
            return self.get_ip(trytimes-1)
        print '未能返回ip'
        return None

    def login(self, ip):
        data = {
            'uname': 'lcx2012',
            'pwd': 'a2Fuemh1bkAxMjM=',
            'remember': 'false',
            'vocode': '',
            'isKf5': 'false'
        }

        s = requests.session()
        url = 'http://account.5118.com/account/LoginConfirm'
        try:
            s.post(url, data=data, timeout=10, proxies=ip)
            # time.sleep(1)
            r = s.get('http://account.5118.com/signin/invitation', allow_redirects=False,timeout=5, proxies=ip)
        except Exception as e:
            print str(e)
            pass
        else:
            if r.status_code == 200:
                selector = etree.HTML(r.text)
                node = selector.xpath('//div[@class="suoming tips2"]')
                if node:
                    if re.search(u'剩余(\d+)天到期', node[0].xpath('string()').strip()):
                        print 'ip登入成功'
                        return s
            # if r.status_code==403:
            #     time.sleep(120)
            print r.status_code
        print 'ip登入失败'
        return None

    def ip_test(self,ip,s,url='http://www.5118.com/seo/related/%E9%98%BF%E9%87%8C',trytimes=2):
        try:
            r=s.get(url,timeout=3,proxies=ip,allow_redirects=False)
        except Exception as e:
            print str(e)
            pass
        else:
            if r.status_code==200 and (u'阿里' in r.text or u'百度' in r.text):
                return True
            print r.status_code
        if trytimes>0:
            return self.ip_test(ip,s,url='http://www.5118.com/seo/related/%E7%99%BE%E5%BA%A6',trytimes=trytimes-1)
        return False

# ip_manager=IpManager()
# ip=ip_manager.get_ip()

# r=requests.get('https://www.baidu.com/')
# r.encoding='utf-8'
# print r.text
# data = {
#     'uname': 'lcx2012',
#     'pwd': 'a2Fuemh1bkAxMjM=',
#     'remember': 'false',
#     'vocode': '',
#     'isKf5': 'false'
# }
# s = requests.session()
# url='http://account.5118.com/account/LoginConfirm'
# r=s.post(url, data=data, timeout=10)
# r=s.get('http://account.5118.com/signin/invitation', allow_redirects=False)
# r=s.get('http://www.5118.com/seo/related/%E9%98%BF%E9%87%8C',timeout=3,allow_redirects=False)

# print r.text