# -*- coding:utf-8 -*-
import sys,re
reload(sys)
sys.setdefaultencoding( "utf-8" )
import MySQLdb,requests,time,re,datetime,json,platform,os
from threading import Lock, Thread, Timer
from Queue import Queue
from atexit import register
from lxml import etree
from ip_manager import IpManager

mac=platform.node()=='MacBook' or 'wangchangtong' in platform.node()
def open_db():
    name="spider"
    if mac:
        db = MySQLdb.connect(db=name, port=3308,host='127.0.0.1', user="spider",passwd="b@4RkJFo!6yL", charset="utf8")
    else:
        db = MySQLdb.connect(db=name, port=3306,host='10.51.178.150', user="spider",passwd="b@4RkJFo!6yL", charset="utf8")
    return db
import redis

pool=None
def open_redis():
    global pool
    if pool:
        r = redis.Redis(connection_pool=pool)
    else:
        pool = redis.ConnectionPool(host='127.0.0.1', port=16379,db=0)
        return open_redis()
    return r

num=zore=have=san_count=out_count=0
ip_manager = IpManager()
ip,s = ip_manager.get_ip()

def switch_ip(q,url):
    global ip,s,san_count,out_count
    while True:
        try:
            ip, s = ip_manager.get_ip()
        except:
            print '获取ip失败，退出'
            requests.get('http://alarm.bosszhipin.com/useralarm/?user=%s&media=wechat&subject=%s&message=获取ip失败，退出' % ('wangchangtong', platform.node()))
            q.put(-1)
            break
        else:
            time.sleep(6)
            for i in xrange(15):
                try:
                    s.get(url, allow_redirects=False, timeout=3, proxies=ip, headers=ip_manager.headers)
                except:
                    time.sleep(3)
                else:
                    break
            if i<14:
                print '切换成功，try:%s' % (i + 1)
                break
            else:
                print '切换失败，重试'
                time.sleep(5)
    san_count=out_count=0

def loop(q,l):
    global num,zore,have,san_count,out_count,ip,s,is_working
    conn=open_db()
    conn.autocommit(True)
    cursor = conn.cursor()
    while True:
        items=q.get()
        if items==-1:
            q.put(-1)
            print '退出:%s'%datetime.datetime.now().strftime('%H:%M:%S')
            break
        url,id,trytimes=items
        if trytimes>30:
            print '重试次数太多，舍弃:%s'%url
            continue
        try:
            r = s.get(url, allow_redirects=False, timeout=4,proxies=ip,headers=ip_manager.headers)
        except Exception as e:
            if 'out' not in str(e):
                print str(e)
            with l:
                q.put((url,id,trytimes+1))
                out_count+=1
                if out_count > 300:
                    print '--timeout--'
                    switch_ip(q,url)
            continue
        if r.status_code==302:
            with l:
                q.put((url,id,trytimes+1))
                san_count+=1
                if san_count > 150:
                    print '--302--'
                    switch_ip(q, url)
            continue
        if r.status_code in (400,404,503):
            print r.status_code
            continue
        if r.status_code==500:
            print 500
            q.put((url, id, trytimes+1))
            continue
        if r.status_code==403:
            print 403
            continue
        if r.status_code==200:
            m = re.search(r'/related/(.+)\?isPager=true', url)
            co_name = m.group(1)
            # 可能会有bug
            if co_name in r.text:
                # print 1
                is_working=True

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
                            q.put((url,id,0))
                        save_co(cursor,co_name,id,url,amount)
                    # 解析
                    parse(id,r.text,cursor)

                else:
                    with l:
                        zore+=1
            with l:
                num+=1
                if num%1000==0:
                    san_count=0
                    out_count=0
                    print '【%s %s %s %s】'%(datetime.datetime.now().strftime('%H:%M:%S'),num,have,zore)
        else:
            print r.status_code,url
    conn.close()

def save_co(cursor,co_name,id,url,amount):
    co_name = co_name.replace('"', '""')
    try:
        cursor.execute('insert into 5118_co VALUES (DEFAULT ,"%s","%s","%s")'%(id,co_name,amount))
    except:
        pass

is_working=True
init=True
t1=0
def parse(id,text,cursor):
    selector=etree.HTML(text)
    items=selector.xpath("//dl[not(@class)]")
    global is_working
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
            try:
                bd_index = values_0[1].xpath("./span/text()")[0]
                if bd_index == '-':
                    bd_index = 0
                result = values_0[2].xpath("./span/text()")[0]
                if result == '-':
                    result = 0
            except:
                continue
        else:
            continue
        try:
            bid_co = item.xpath("./dd[@class='col3-4 center']/a/text()")[0]
        except:
            continue
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

def product_loop(q,timer=0):
    time.sleep(timer)
    size=q.qsize()
    print '检查queue,size:%s。%s'%(size, datetime.datetime.now().strftime('%H:%M:%S'))
    global t1,is_working
    if size==0:
        r=open_redis()
        global init
        if init and os.path.exists('old_task.txt'):
            num=0
            with open('old_task.txt') as f:
                for line in f.readlines():
                    url,id=line.split('--0--')
                    num+=1
                    q.put((url,id,0))
            print '从文件中读取到【%s】条种子'%num
        else:
            # t2=time.time()
            if not is_working:
                requests.get('http://alarm.bosszhipin.com/useralarm/?user=%s&media=wechat&subject=%s退出信号发送&message=防无限取redis退出' % ('wangchangtong', platform.node()))
                print '防无限取redis，退出'
                q.put(-1)
                return
            # else:
            #     t1=t2
            with open('old_task.txt', 'w') as f:
                print '从redis中获取500条种子'
                for _ in xrange(500):
                    line=r.lpop('5118')
                    if line==None:
                        print '未获取到任务'
                        q.put(-1)
                        break
                    f.write(line+'\n')
                    url, id = line.split('--0--')
                    q.put((url, id,0))
                print '获取完成！！！'
            size = r.llen('5118')
            requests.get('http://alarm.bosszhipin.com/useralarm/?user=%s&media=wechat&subject=500&message=%s从redis中取走500，剩余：%s' % ('wangchangtong',platform.node(),size))
            is_working=False
        init=False

    open_redis()
    timer=(q.qsize()+24)/12
    if timer>300:
        timer=300
    print '%s秒后检查'%timer
    if datetime.datetime.now().strftime('%H')=='2':
        print '两点了，退出'
        q.put(-1)
        requests.get('http://alarm.bosszhipin.com/useralarm/?user=%s&media=wechat&subject=%s退出信号发送&message=两点' % ('wangchangtong',platform.node()))
        return
    # Timer(timer,product_loop,args=(q,)).start()
    t = Thread(target=product_loop,args=(q,timer))
    t.setDaemon(True)
    t.start()

def start():
    lock=Lock()
    q=Queue()
    t=Thread(target=product_loop,args=(q,))
    t.setDaemon(True)
    t.start()
    for _ in range(50):
        Thread(target=loop,args=(q,lock)).start()
start()

@register
def f():
    requests.get('http://alarm.bosszhipin.com/useralarm/?user=%s&media=wechat&subject=%s退出&message=退出啦' % ('wangchangtong',platform.node()))