# -*- coding:utf-8 -*-
import sys,re
reload(sys)
sys.setdefaultencoding( "utf-8" )
import MySQLdb,requests,time,re,datetime,json
from threading import Lock,Thread
from Queue import Queue
from atexit import register
from lxml import etree
from ip_manager import IpManager

def open_db():
    conn = MySQLdb.connect(db='spider', port=3306,host='127.0.0.1', user="root",passwd="w3223214", charset="utf8")
    # conn = MySQLdb.connect(db='dfetcher', port=3306,host='127.0.0.1', user="root",passwd="w3223214", charset="utf8")
    # conn = MySQLdb.connect(db='spider', port=3306, host='10.51.178.150', user="spider", passwd="b@4RkJFo!6yL", charset="utf8")
    return conn


num=0
zore=0
have=0
san_count=0
out_count=0
ip_manager = IpManager()
ip,s = ip_manager.get_ip()


def loop(q,l):
    global num
    global zore
    global have
    global san_count
    global out_count
    global ip
    global s
    conn=open_db()
    conn.autocommit(True)
    cursor = conn.cursor()
    while True:
        try:
            items=q.get(timeout=3)
        except Exception:
            break
        url,id=items
        try:
            r = s.get(url, allow_redirects=False, timeout=3,proxies=ip,headers=ip_manager.headers)
        except Exception as e:
            print str(e)
            with l:
                out_count+=1
                if out_count > 200:
                    ip, s = ip_manager.get_ip()
                    out_count = 0
                    san_count = 0
                q.put((url,id))
            continue
        if r.status_code==302:
            with l:
                san_count+=1
                if san_count > 200:
                    ip, s = ip_manager.get_ip()
                    san_count = 0
                    out_count = 0
                q.put((url,id))
            continue
        if r.status_code in (400,404,503):
            print r.status_code
            continue
        if r.status_code==403:
            print 403
            q.put((url, id))
            continue
        if r.status_code==200:
            a = re.search(r"DownParams.PayRowCount = '(\d+)'", r.text)
            if a:
                amount=int(a.group(1))
                if amount!=0:
                    page=re.search(r'pageIndex=(\d+)',url)
                    page=int(page.group(1))
                    if page==1:
                        with l:
                            have+=1
                        max=(amount-1)//50+1
                        if max>20:
                            max=20
                        for i in range(2,max+1):
                            index = url.find('pageIndex=')
                            url=url[:index+10]+str(i)
                            q.put((url,id))
                        save_co(cursor,id,url,amount)
                    # 解析
                    parse(id,amount,r.text,cursor)

                else:
                    with l:
                        zore+=1
            with l:
                num+=1
                if num%1000==0:
                    san_count=0
                    out_count=0
                    print datetime.datetime.now().strftime('%H:%M:%S'),num,have,zore
        else:
            print r.status_code,url
    conn.close()
def save_co(cursor,id,url,amount):
    m = re.search(r'/related/(.+)\?isPager=true', url)
    co_name = m.group(1)
    co_name = co_name.replace('"', '""')
    try:
        cursor.execute('insert into 5118_co VALUES (DEFAULT ,"%s","%s","%s")'%(id,co_name,amount))
    except:
        pass

def parse(id, amonut, text,cursor):
    selector=etree.HTML(text)
    items=selector.xpath("//dl[not(@class)]")
    for item in items:
        em=item.xpath(".//span[@class='hoverToHide']//em[@class='relate-hightlight']")
        if em:
            try:
                word=em[0].xpath("./text()")[0]
            except:
                continue
        else:
            word=item.xpath(".//span[@class='hoverToHide']/a/text()")
            if word:
                word=word[0]
            else:
                continue
        word=word.replace('"','""')
        values_0 = item.xpath("./dd[@class='col3-5 center']")
        if len(values_0) == 3:
            bd_index = values_0[1].xpath("./span/text()")[0]
            if bd_index == '-':
                bd_index = 0
            result = values_0[2].xpath("./span/text()")[0]
            if result == '-':
                result = 0
        else:
            continue
        bid_co = item.xpath("./dd[@class='col3-4 center']/a/text()")[0]
        values_1 = item.xpath("./dd[@class='col3-3 center jingjia-info-dd']")
        if len(values_1) == 2:
            pc = values_1[0].xpath("./text()")[0].strip()
            mobile = values_1[1].xpath("./text()")[0].strip()
        else:
            continue
        bid_level = item.xpath("./dd[@class='col3-4 center jingjia-progress-dd jingjia-info-dd']/span/text()")
        if bid_level:
            bid_level = bid_level[0].strip()
        else:
            continue
        try:
            cursor.execute('insert into 5118_data VALUES (DEFAULT ,"%s","%s","%s","%s","%s","%s","%s","%s")'%(id,word,bd_index,result,bid_co,pc,mobile,bid_level))
        except:
            pass

def start():
    lock=Lock()
    q=Queue()
    count=0
    with open('data_content_1620.txt') as f:
        f.readline()
        for line in f.readlines():
            count +=1
            if count==10000:
                break
            url,id=line.split('\t')
            q.put((url,int(id)))
    for _ in range(50):
        Thread(target=loop,args=(q,lock)).start()
start()

@register
def f():
    print have,zore,san_count


# url='http://www.5118.com/seo/related/Ziver 智物通讯?isPager=true&pageIndex=40'
# page=re.search(r'pageIndex=(\d+)',url)
# page=int(page.group(1))
# print page
# url=url.replace(r'pageIndex=(\d+)','2')
# print url
# n=1
# print (n-1)//50+1
# index = url.find('pageIndex=')
# print url[:index+10]+str(2)

# print len('\t
# print re.match('\d+',int('111'))
#
# int('aa')
# while True:
#     num+=1
#     print num
#     r=s.get('http://www.5118.com/seo/related/Sergiorossi?isPager=true&pageIndex=1',allow_redirects=False)
#     if r.status_code!=200:
#         print r.status_code
#         print r.text
#         break
# max=3
# for i in range(2,max+1):
#     print i
# with open('data_content_1620.txt') as f:
#     num=0
#     f.readline()
#     for line in f.readlines():
#         # num+=1
#         # print num
#         # if num==100:
#         #     break
#         # try:
#         url, id = line.split('\t')
#         m=re.search(r'/related/(.+)\?isPager=true',url)
#         print m.group(1)
#         # except Exception:
#         #     print url
# a='http://www.5118.com/seo/related/%E5%8C%97%E4%BA%AC?isPager=true&pageIndex=1'
# r=s.get(a, allow_redirects=False, timeout=3,proxies=proxies_1)
# selector=etree.HTML(r.text)
# items=selector.xpath("//dl[not(@class)]")
# for item in items:
#     em=item.xpath(".//span[@class='hoverToHide']//em[@class='relate-hightlight']")
#     if em:
#         try:
#             word=em[0].xpath("./text()")[0]
#         except Exception:
#             print '没有这个,%s %s'%(url)
#     else:
#         word=item.xpath(".//span[@class='hoverToHide']/a/text()")
#         if word:
#             word=word[0]
#
#     values_0=item.xpath("./dd[@class='col3-5 center']")
#     if len(values_0)==3:
#         bd_index=values_0[1].xpath("./span/text()")[0]
#         if bd_index=='-':
#             bd_index=0
#         result=values_0[2].xpath("./span/text()")[0]
#         if result=='-':
#             result=0
#     else:
#         print '-len!=3,%s,%s'%(url,p)
#         continue
#     bid_co=item.xpath("./dd[@class='col3-4 center']/a/text()")[0]
#     values_1=item.xpath("./dd[@class='col3-3 center jingjia-info-dd']")
#     if len(values_1)==2:
#         pc=values_1[0].xpath("./text()")[0].strip()
#         mobile=values_1[1].xpath("./text()")[0].strip()
#     else:
#         print '*len!=2,%s,%s' % (url, p)
#         continue
#     bid_lever=item.xpath("./dd[@class='col3-4 center jingjia-progress-dd jingjia-info-dd']/span/text()")
#     if bid_lever:
#         bid_lever=bid_lever[0].strip()
#     else:
#         continue
#
#     print word,bd_index,result,bid_co,pc,mobile,bid_lever