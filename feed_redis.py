# -*- coding:utf-8 -*-
import sys,os,datetime,time
reload(sys)
sys.setdefaultencoding( "utf-8" )
__author__ = 'tongtong'
import redis,platform,threading,Queue
pool=None
mac=platform.node()=='MacBook'
def open_redis():
    global pool
    if pool:
        r = redis.Redis(connection_pool=pool)
    else:
        if mac:
            pool = redis.ConnectionPool(host='127.0.0.1', port=16379,db=0)
        else:
            pool = redis.ConnectionPool(host='127.0.0.1', port=16379,db=0)

        return open_redis()
    return r

def feed():
    q=Queue.Queue()
    l=threading.Lock()
    with open('data_content_1620.txt','r') as f:
        for line in f.readlines():
            v=line.split('\t')
            v='--0--'.join(v).strip()
            q.put(v)
    q.put(-1)
    for _ in range(40):
        threading.Thread(target=loop,args=(q,l)).start()
num=0
def loop(q,l):
    global num
    r=open_redis()
    while True:
        item=q.get()
        if item==-1:
            q.put(-1)
            break
        with l:
            num+=1
            print num
        r.rpush('5118',item)

def get():
    r = open_redis()
    for _ in range(10):
        print r.lpop('5118')
def delete():
    r=open_redis()
    r.flushall()
# delete()
feed()
# get()
# r=open_redis()
# print type(r.lpop('5118'))
# print r.lpop('5118')
