# -*- coding:utf-8 -*-
import sys,os,datetime,time
reload(sys)
sys.setdefaultencoding( "utf-8" )
__author__ = 'tongtong'
import redis
pool=None
def _open_redis():
    global pool
    if pool:
        r = redis.Redis(connection_pool=pool)
    else:
        pool = redis.ConnectionPool(host='127.0.0.1', port=6379,db=0)
        return _open_redis()
    return r

def feed():
    r=_open_redis()
    with open('data_content_1620.txt','r') as f:
        for line in f.readlines():
            v=line.split('\t')
            if '--0--' in v[0]:
                print v[0]
            v='--0--'.join(v).strip()
            r.rpush('5118',v)
# feed()
def get():
    r = _open_redis()
    for _ in range(10):
        print r.lpop('5118')
def delete():
    r=_open_redis()
    r.flushall()
delete()
feed()
# get()
