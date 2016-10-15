
# coding: utf-8

"""
@author: wisonhang
@site: hangdawei@126.com
"""
# reference ： http://lanbing510.info/2016/03/15/Lianjia-Spider.html

from bs4 import BeautifulSoup
import requests
import random
import sqlite3
import random
import threading
import sys
import urllib
import time
from numpy import *
from pandas import *

from log_info.bj import *  #导入登录信息  北京  
#from log_info.hz import * #导入登录信息  杭州
#from log_info.sz import * #导入登录信息  深圳
#from log_info.gz import * #导入登录信息  广州

######################北京链家 导入示例###
'''
cookies={
    'lianjia_uuid':'99949991-e9c7-4df8-b8bc-7d427ccf98ad',
    '_ga':'GA1.2.1956483913.1473815618',
    'gr_user_id':'84a5ae61-9280-4505-b0a2-ebdca807fe32',
    'ubta':'2299869246.3823483042.1473815618645.1475972328299.1475972333450.185',
    'pt_393d1d99':'uid=El7-zd3kPbVUnu-qmjjIng&nid=0&vid=COojRQz1TWAI6doA/t-01w&vn=1&pvn=6&sact=1474799546672&to_flag=0&pl=lI2h1cVGgrwUeTf-92t3kQ*pt*1474799418062', 
    '_smt_uid':'57f5eb50.334841a2', 'a7122_times':'1', 'CNZZDATA1253477573':'1879280474-1475798825-%7C1475971762', 
    'CNZZDATA1254525948':'1826278568-1475800458-%7C1475973258', 'CNZZDATA1255633284':'1268179366-1475796095-%7C1475974302', 
    'select_city':'110000', 'cityCode':'bj', 'ubt_load_interval_b':'1475929188945', 
    'ubtc':'2299869246.3823483042.1475972333459.6C28FC5109B0462DADAD43CCC12812E0', 'ubtd':'52', 'lianjia_token':'2.0024eab9f55df1cf6c354790c4f2400ad7', 
    '_jzqa':'1.1393651109478892300.1475901567.1475903927.1475910449.3', '_jzqc':'1', '_jzqckmp':'1', 
    'all-lj':'3e56656136803bc056cf7a329e54869e', 'lianjia_ssid':'beef9d5f-44c0-49da-abe5-cea4d7e53a5d', 
    'CNZZDATA1255604082':'1589290616-1475971693-%7C1475971693'    
}
headers={
'Host': 'bj.lianjia.com',
'User-Agent':' Mozilla/5.0 (Windows NT 6.3; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding': 'gzip, deflate',
'Referer': 'http://bj.lianjia.com/chengjiao/',
'Connection': 'keep-alive',
'Upgrade-Insecure-Requests': '1',
'Cache-Control': 'max-age=0'
}


#北京区域列表
regionsb=["东城","西城","朝阳","海淀","丰台","石景山","通州","昌平","大兴",
"亦庄开发区","顺义","房山","门头沟","平谷","怀柔","密云","延庆","燕郊"]

xiaoqulink="http://bj.lianjia.com/xiaoqu/rs"
xiaoqulink_page="http://bj.lianjia.com/xiaoqu/pg%srs%s/" 
chengjiaolink="http://bj.lianjia.com/chengjiao/rs"
chengjiaolink_page="http://bj.lianjia.com/chengjiao/pg%srs%s/"
'''
####################################################
lock = threading.Lock()
class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock() #锁  
        self.path = path #数据库连接参数  

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    def get_conn(self):  
        conn = sqlite3.connect(self.path)#,check_same_thread=False)  
        conn.text_factory=str
        return conn   

    def conn_close(self,conn=None):  
        conn.close()  

    def conn_trans(func):  
        def connection(self,*args,**kwargs):  
            self.lock.acquire()  
            conn = self.get_conn()  
            kwargs['conn'] = conn  
            rs = func(self,*args,**kwargs)  
            self.conn_close(conn)  
            self.lock.release()  
            return rs  
        return connection  

    @conn_trans    
    def execute(self,command,method_flag=0,conn=None):  
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError as e:
            #print (e)
            return -1
        except Exception as e:
            #print (e)
            return -2
        return 0

    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception as e:
            print (e)
            pass
        return lists
######################################################################
def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=['小区名称','大区域','小区域','户型','结构','建造时间','挂牌均价','在售','成交','出租','学区','地铁']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def xiaoqu_spider(db_xq,url_page="http://bj.lianjia.com/xiaoqu/"):
    """
    爬取页面链接中的小区信息
    """
    time.sleep(10)
    try:
        s=requests.session()
        r=s.post(url_page,headers=headers,cookies=cookies,timeout=60)
        soup=BeautifulSoup(r.content.decode('utf-8','ignore'),'lxml')  #此处 decode 需要加 ignore 
        xiaoqu_list=soup.findAll('li',attrs={'class':'clear'})
    except Exception as e:
        print (e)
        return
    for xq in xiaoqu_list:
        info_dict={}
        info_dict.update({'小区名称':xq.find('div',attrs={'class':'title'}).text})
        info=xq.find('div',attrs={'class':'houseInfo'}).text.split('|')
        if info:
            for ct in info:
                if ct.find('成交')!=-1:
                    info_dict.update({'成交':ct})
                if ct.find('户型')!=-1:
                    info_dict.update({'户型':ct})
                if ct.find('出租')!=-1:
                    info_dict.update({'出租':ct})
        info=xq.find('div',attrs={'class':'positionInfo'}).text.split('\xa0')
        if info:
            info_dict.update({'大区域':info[0]})
            info_dict.update({'小区域':info[1]})
            info_dict.update({'结构':info[2][1:-1]})
            info_dict.update({'建造时间':info[3][0:info[3].index('年')]})
        info_dict.update({'挂牌均价':xq.find('div',attrs={'class':'totalPrice'}).text})
        info_dict.update({'在售':xq.find('a',attrs={'class':'totalSellCount'}).text})
        info=xq.find('div',attrs={'class':'tagList'})
        if info:
            content=info.text.split()
            for c in content:
                    if c.find('地铁')!=-1:
                        info_dict.update({'地铁':c})
                    elif c.find('学')!=-1:
                        info_dict.update({'学区':c})
        command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)

def do_xiaoqu_spider(db_xq,region="朝阳",base_url=xiaoqulink,url_page=xiaoqulink_page):
    """
    爬取大区域中的所有小区信息
    """
    url=base_url+region+"/"
    try:
        s=requests.session()
        r=s.get(url,headers=headers,cookies=cookies,timeout=60)
        soup=BeautifulSoup(r.content.decode('utf-8','ignore'),'lxml')
        num=soup.find('h2',attrs={'class':'total fl'}).span.text
        total_pages=int(ceil(float(num)/30))
    except Exception as e:
        print (e)
        return
    else:
        threads=[]
        for i in range(total_pages):
            url_page=url_page%(i+1,region)
        #print(url_page)
            t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    print ("爬下了 %s 区全部的小区信息" % region)



######################################################################
def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=['链接','小区名称','户型','面积','区域','街道','朝向','装潢','楼层','设施','建成',
               '签约时间','签约单价','签约总价','房产类型','学区','地铁']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def chengjiao_spider(db_cj,db_xq,area,page,url=chengjiaolink_page):
    """
    爬取页面链接中的成交记录 url="http://xx.lianjia.com/chengjiao/pg%srs%s/" 
    """
    time.sleep(20)
    try:
        url_page=url%(page,area)
        #print(url_page)
        s=requests.session()
        r=s.get(url_page,headers=headers,cookies=cookies,timeout=60)
        soup=BeautifulSoup(r.content.decode('utf-8','ignore'),'lxml')
        cj_list=soup.findAll('ul',{'class':'listContent'})[0].findAll('li')
    except Exception as e:
        print (e)
        exception_write('chengjiao_spider',url_page)
        return 
    for cj in cj_list:
        info_dict={}
        href=cj.find('a')
        if not href:
            continue
        info_dict.update({'链接':href['href']})
        content=cj.find('div',attrs={'class':'title'}).text.split()
        if len(content)==3:
            info_dict.update({'小区名称':content[0]})
            try:
                cont=db_xq.fetchall(command="select regionb,regions from xiaoqu where name is"+'"'+info_dict['小区名称']+'"')
                info_dict.update({'区域':cont[0][0]})
                info_dict.update({'街道':cont[0][1]})
            except Exception as e :
                info_dict.update({'街道':area})
                quyu=db_xq.fetchall(command="select distinct regionb from xiaoqu where regions is"+'"'+area+'"')
                if quyu:
                    info_dict.update({'区域':quyu[0][0]})
                else:
                    info_dict.update({'区域':area})
            info_dict.update({'户型':content[1]})
            info_dict.update({'面积':content[2]})
        content=cj.find('div',attrs={'class':'houseInfo'}).text.replace('\xa0', '').replace(' ', '').split('|')
        if content:
            info_dict.update({'朝向':content[0]})
            info_dict.update({'装潢':content[1]})
            if len(content)>2:
                info_dict.update({'设施':content[2]})
        info_dict.update({'签约时间':cj.find('div',attrs={'class':'dealDate'}).text})
        info_dict.update({'签约总价':cj.find('div',attrs={'class':'totalPrice'}).text})
        info_dict.update({'签约单价':cj.find('div',attrs={'class':'unitPrice'}).text})
        content=cj.find('div',attrs={'class':'positionInfo'}).text.split()
        if content:
            info_dict.update({'楼层':content[0]})
            try:
                info_dict.update({'建成':content[1][0:4]})
            except Exception as e:
                pass
        content=cj.find('span',attrs={'class':'dealHouseTxt'})
        if content:
            content=content.text.split()
            for c in content:
                    if c.find('满')!=-1:
                        info_dict.update({'房产类型':c})
                    elif c.find('距')!=-1:
                        info_dict.update({'地铁':c})
                    elif c.find('学')!=-1:
                        info_dict.update({'学区':c})

        command=gen_chengjiao_insert_command(info_dict)
        db_cj.execute(command,1)
        
def xiaoqu_chengjiao_spider(db_cj,db_xq,xq_name="朝阳",url_base=chengjiaolink):
    """
    爬取小区成交记录  url_base="http://xx.lianjia.com/chengjiao/rs"
    """
    url=url_base+urllib.parse.quote(xq_name)+"/"
    try:
        s=requests.session()
        r=s.get(url,headers=headers,cookies=cookies,timeout=60)
        soup=BeautifulSoup(r.content.decode('utf-8','ignore'),'lxml')
        num=soup.find('div',attrs={'class':'total fl'}).span.text
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print (e)
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    except Exception as e:
        print (e)
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    else:
        total_pages=int(ceil(float(num)/30))
        threads=[]
        for i in range(total_pages):
            page=i+1
            t=threading.Thread(target=chengjiao_spider,args=(db_cj,db_xq,xq_name,page))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

def do_xiaoqu_chengjiao_spider(db_cj,db_xq,command="select distict name from xiaoqu"):
    """
    批量爬取小区成交记录
    """
    count=0
    xq_list=db_xq.fetchall(command)
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj,db_xq,xq[0])
        count+=1
        print ('have spidered %d xiaoqu' % count)
    print ('done')
    
    
    
    
def xiaoqu_chenjiao_update(db_cj,db_xq,con="select district, count(road) from chengjiao GROUP by district",url_base=chengjiaolink):
    data=db_cj.fetchall(con)
    for xq_name,val in data:
        url=url_base+urllib.parse.quote(xq_name)+"/"
        try:
            s=requests.session()
            r=s.get(url,headers=headers,cookies=cookies)
            soup=BeautifulSoup(r.text,'lxml')
            num=soup.find('div',attrs={'class':'total fl'}).span.text
            total_pages=int(ceil(float(num)-val)/20)
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            print (e)
            exception_write('xiaoqu_chengjiao_spider',xq_name)
            pass
        except Exception as e:
            print (e)
            exception_write('xiaoqu_chengjiao_spider',xq_name)
            pass
        else:
            print(xq_name , ':',val,'downloaded, ',num ,'total have ')
            threads=[]
            for i in range(total_pages):
                page=i+1
                t=threading.Thread(target=chengjiao_spider,args=(db_cj,db_xq,xq_name,page))
                threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
##################################################################
def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f=open('log.txt','r')
    lines=f.readlines()
    f.close()
    f=open('log.txt','w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj,db_xq):
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="chengjiao_spider":
                page=url[url.find("pg")+2:url.find("rs")]
                area=url[url.find("rs")+2:url.rindex("/")]
                chengjiao_spider(db_cj,db_xq,area,page)
                count+=1
            elif excep_name=="xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,db_xq,url)
                count+=1
            else:
                print ("wrong format")
            print ("have spidered %d exception url" % count)
        excep_list=exception_read()
    print ('all done ^_^')
    
    
    


########################################################################

if __name__=="__main__":
    #['小区名称','大区域','小区域','户型','结构','建造时间','挂牌均价','在售','成交','出租','地铁']
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, style TEXT, jieogu Text,year TEXT,price TEXT,sell TEXT,chengjiao TEXT, chuzu TEXT, school TEXT ,subway TEXT)"   
    db_xq=SQLiteWraper('bj-xiaoqu.db',command)
    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT,district TEXT, road TEXT, orientation TEXT ,zhuanghuang TEXT, floor TEXT, sheshi TEXT,year TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT)"  
    db_cj=SQLiteWraper('bj-chengjiao.db',command)
    
    for region in regionsb:
        do_xiaoqu_spider(db_xq,region)
        
    do_xiaoqu_chengjiao_spider(db_cj,db_xq,command='select distinct regions from xiaoqu')
    
    exception_spider(db_cj,db_xq)
