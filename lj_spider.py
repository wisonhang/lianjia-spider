
# coding: utf-8

"""
@author: wisonhang
@site: hangdawei@126.com
"""

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

from log_info.bj import *
#from log_info.hz import *
#from log_info.sz import *
#from log_info.gz import *

######################北京链家##############################################
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
regionsb=[u"东城",u"西城",u"朝阳",u"海淀",u"丰台",u"石景山","通州",u"昌平",u"大兴",u"亦庄开发区",u"顺义",u"房山",u"门头沟",u"平谷",u"怀柔",u"密云",u"延庆",u"燕郊"]

xiaoqulink="http://bj.lianjia.com/xiaoqu/rs"
xiaoqulink1="http://bj.lianjia.com/xiaoqu/pg%srs%s/" 
chengjiaolink="http://bj.lianjia.com/chengjiao/rs"
chengjiaolink1="http://bj.lianjia.com/chengjiao/pg%srs%s/"
'''

###################深圳链家#####################################################################
'''
cookies={
"lianjia_uuid":"99949991-e9c7-4df8-b8bc-7d427ccf98ad",
" _ga":"GA1.2.1956483913.1473815618"," gr_user_id":"84a5ae61-9280-4505-b0a2-ebdca807fe32"," ubta":"3801124163.3823483042.1473815618645.1476006264004.1476006266602.196"," _smt_uid":"57f5eb50.334841a2"," _jzqa":"1.1393651109478892300.1475901567.1475903927.1475910449.3",
"select_city":"440300"," lianjia_ssid":"cd65a8c9-890f-473b-b90b-3e98dc615851"," all-lj":"007e0800fb44885aa2065c6dfaaa4029"," CNZZDATA1255849469":"1536051407-1476158297-%7C1476169097"," CNZZDATA1254525948":"1927274610-1476162258-%7C1476167658"," CNZZDATA1255633284":"1546268043-1476157913-%7C1476168713"," CNZZDATA1255604082":"927070903-1476160697-%7C1476171497"," select_nation":"1"," _gat":"1"," _gat_global":"1"," _gat_new_global":"1"," _gat_dianpu_agent":"1"," _gat_past":"1"," logger_session":"f0c39510d054c5b41b85ca3feec62732"," lianjia_token":"2.0046a9abb13fb2dd285704828064bd7816","pt_393d1d99":"uid=El7-zd3kPbVUnu-qmjjIng&nid=0&vid=Ovvz6ItolFCaBgB7NKtT8A&vn=2&pvn=6&sact=1476006415413&to_flag=1&pl=VmozlfHU39/Dp1xTBh4m4w*pt*1476006260868"
}
headers={
'Host': 'sz.lianjia.com',
'User-Agent':' Mozilla/5.0 (Windows NT 6.3; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding': 'gzip, deflate',
'Referer': 'http://sz.lianjia.com/chengjiao/',
'Connection': 'keep-alive',
'Upgrade-Insecure-Requests': '1',
'Cache-Control': 'max-age=0'
}
#深圳地区
regionsb=['罗湖','福田','南山','盐田','宝安','龙岗','龙华新区','光明新区','坪山新区','大鹏新区']
xiaoqulink="http://sz.lianjia.com/xiaoqu/rs"
xiaoqulink1="http://sz.lianjia.com/xiaoqu/pg%srs%s/" 
chengjiaolink="http://sz.lianjia.com/chengjiao/rs"
chengjiaolink1="http://sz.lianjia.com/chengjiao/pg%srs%s/"

'''

#####################杭州链家#######################################################################
'''
headers={
'Host': 'hz.lianjia.com',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding': 'gzip, deflate',
'Connection': 'keep-alive',
'Upgrade-Insecure-Requests': '1',
'Cache-Control': 'max-age=0',
}
cookies={
    'lianjia_uuid':'99949991-e9c7-4df8-b8bc-7d427ccf98ad', 
    '_ga':'GA1.2.1956483913.1473815618', 'gr_user_id':'84a5ae61-9280-4505-b0a2-ebdca807fe32',
    'ubta':'2299869246.3823483042.1473815618645.1475897124471.1475901079377.178',
    'pt_393d1d99':'uid=El7-zd3kPbVUnu-qmjjIng&nid=0&vid=COojRQz1TWAI6doA/t-01w&vn=1&pvn=6&sact=1474799546672&to_flag=0&pl=lI2h1cVGgrwUeTf-92t3kQ*pt*1474799418062', 
    '_smt_uid':'57f5eb50.334841a2', 'a7122_times':'3', 'miyue_hide':'%20index%20%20index%20%20index%20%20index%20', 
    'select_city':'330100','cityCode':'hz', 'ubt_load_interval_b':'1475901079004',
    'ubtc':'2299869246.3823483042.1475901079381.11D8788D6D57F6512037C50B045F7530', 
    'ubtd':'45', 'lianjia_token':'2.0024eab9f55df1cf6c354790c4f2400ad7', 
    'all-lj':'eb773d00ac95736a80cb663e13368872', 'sample_traffic_test':'controlled_50', 
    'CNZZDATA1253492436':'1823143126-1475896243-%7C1475901644', 'CNZZDATA1254525948':'328737779-1475892258-%7C1475903058', 
    'CNZZDATA1255633284':'1312673896-1475893301-%7C1475904101', 'CNZZDATA1255604082':'456707277-1475896075-%7C1475901475', 
    '_jzqa':'1.1393651109478892300.1475901567.1475901567.1475903927.2', '_jzqc':'1', '_jzqckmp':'1', 
    '_qzja':'1.1045321316.1475901567379.1475901567380.1475903926920.1475904004480.1475904293041.0.0.0.5.2', '_qzjc':'1', 
    '_qzjto':'5.2.0', 'lianjia_ssid':'76e0bac7-063d-48e6-9b31-ec997988e7c1', '_jzqb':'1.3.10.1475903927.1', 
    '_qzjb':'1.1475903926920.3.0.0.0', '_gat_past':'1', '_gat_global':'1', '_gat_new_global':'1', '_gat_dianpu_agent':'1'
}
regionsb=['西湖','下城','江干','拱墅','上城','滨江','余杭','萧山']


xiaoqulink="http://hz.lianjia.com/xiaoqu/rs"
xiaoqulink1="http://hz.lianjia.com/xiaoqu/pg%srs%s/" 
chengjiaolink="http://hz.lianjia.com/chengjiao/rs"
chengjiaolink1="http://hz.lianjia.com/chengjiao/pg%srs%s/"

'''


######################################广州链家#############################################################

'''
cookies={"lianjia_uuid":"99949991-e9c7-4df8-b8bc-7d427ccf98ad"," _ga":"GA1.2.1956483913.1473815618"," gr_user_id":"84a5ae61-9280-4505-b0a2-ebdca807fe32"," ubta":"2299869246.3823483042.1473815618645.1476201738927.1476238300201.208"," _smt_uid":"57f5eb50.334841a2"," _jzqa":"1.1393651109478892300.1475901567.1475903927.1475910449.3"," select_city":"440100"," CNZZDATA1255849599":"185235040-1476165937-%7C1476274738"," CNZZDATA1254525948":"39492236-1476167658-%7C1476275658"," CNZZDATA1255633284":"1038784991-1476163313-%7C1476276715"," CNZZDATA1255604082":"1756489217-1476166097-%7C1476274097","pt_393d1d99":"uid=El7-zd3kPbVUnu-qmjjIng&nid=0&vid=VhrzEub-dVXgQmARnCLUbw&vn=3&pvn=1&sact=1476173324748&to_flag=0&pl=GNyAMuX/NzpdMjK1V4e58A*pt*1476173323004","lianjia_token":"2.004c89aba93592dd305d2482987f0b97d2"," all-lj":"0a26bbdedef5bd9e71c728e50ba283a3"," lianjia_ssid":"464bfbde-a46d-497b-90cc-d982314f91b8"," _gat":"1"," _gat_global":"1"," _gat_new_global":"1"," _gat_dianpu_agent":"1"}

headers={
'Host': 'gz.lianjia.com',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding': 'gzip, deflate',
'Referer': 'http://gz.lianjia.com/chengjiao/',
'Connection': 'keep-alive',
'Upgrade-Insecure-Requests': '1',
'Cache-Control':'max-age=0'
    }

regionsb=['天河','越秀','荔湾','海珠','番禺','白云','黄埔','从化','增城','花都','萝岗','南沙']

xiaoqulink="http://gz.lianjia.com/xiaoqu/rs"
xiaoqulink1="http://gz.lianjia.com/xiaoqu/pg%srs%s/" 
chengjiaolink="http://gz.lianjia.com/chengjiao/rs"
chengjiaolink1="http://gz.lianjia.com/chengjiao/pg%srs%s/"
'''
######################################################################################################
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
    info_list=[u'小区名称',u'大区域',u'小区域',u'户型',u'结构',u'建造时间',u'挂牌均价',u'在售',u'成交',u'出租',u'学区',u'地铁']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def xiaoqu_spider(db_xq,url_page=u"http://bj.lianjia.com/xiaoqu/"):
    """
    爬取页面链接中的小区信息
    """
    time.sleep(10)
    try:
        s=requests.session()
        r=s.post(url_page,headers=headers,cookies=cookies,timeout=60)
        soup=BeautifulSoup(r.content.decode('utf-8','ignore'),'lxml')
        xiaoqu_list=soup.findAll('li',attrs={'class':'clear'})
    except Exception as e:
        print (e)
        return
    for xq in xiaoqu_list:
        info_dict={}
        info_dict.update({u'小区名称':xq.find('div',attrs={'class':'title'}).text})
        info=xq.find('div',attrs={'class':'houseInfo'}).text.split('|')
        if info:
            for ct in info:
                if ct.find('成交')!=-1:
                    info_dict.update({u'成交':ct})
                if ct.find('户型')!=-1:
                    info_dict.update({u'户型':ct})
                if ct.find('出租')!=-1:
                    info_dict.update({u'出租':ct})
        info=xq.find('div',attrs={'class':'positionInfo'}).text.split('\xa0')
        if info:
            info_dict.update({u'大区域':info[0]})
            info_dict.update({u'小区域':info[1]})
            info_dict.update({u'结构':info[2][1:-1]})
            info_dict.update({u'建造时间':info[3][0:info[3].index('年')]})
        info_dict.update({u'挂牌均价':xq.find('div',attrs={'class':'totalPrice'}).text})
        info_dict.update({u'在售':xq.find('a',attrs={'class':'totalSellCount'}).text})
        info=xq.find('div',attrs={'class':'tagList'})
        if info:
            content=info.text.split()
            for c in content:
                    if c.find(u'地铁')!=-1:
                        info_dict.update({u'地铁':c})
                    elif c.find(u'学')!=-1:
                        info_dict.update({u'学区':c})
        command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)

def do_xiaoqu_spider(db_xq,region=u"朝阳"):
    """
    爬取大区域中的所有小区信息
    """
    url=xiaoqulink+region+"/"
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
            url_page=xiaoqulink1%(i+1,region)
        #print(url_page)
            t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    print (u"爬下了 %s 区全部的小区信息" % region)



######################################################################
def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'链接',u'小区名称',u'户型',u'面积',u'区域',u'街道',u'朝向',u'装潢',u'楼层',u'设施',u'建成',
               u'签约时间',u'签约单价',u'签约总价',u'房产类型',u'学区',u'地铁']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def chengjiao_spider(db_cj,db_xq,area,page,url=chengjiaolink1):
    """
    爬取页面链接中的成交记录
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
    #cj_list=soup.findAll('ul',{'class':'listContent'})[0].findAll('li')
    for cj in cj_list:
        info_dict={}
        href=cj.find('a')
        if not href:
            continue
        info_dict.update({u'链接':href['href']})
        content=cj.find('div',attrs={'class':'title'}).text.split()
        if len(content)==3:
            info_dict.update({u'小区名称':content[0]})
            try:
                cont=db_xq.fetchall(command="select regionb,regions from xiaoqu where name is"+'"'+info_dict['小区名称']+'"')
                info_dict.update({u'区域':cont[0][0]})
                info_dict.update({u'街道':cont[0][1]})
            except Exception as e :
                info_dict.update({u'街道':area})
                quyu=db_xq.fetchall(command="select distinct regionb from xiaoqu where regions is"+'"'+area+'"')
                if quyu:
                    info_dict.update({u'区域':quyu[0][0]})
                else:
                    info_dict.update({u'区域':area})
            info_dict.update({u'户型':content[1]})
            info_dict.update({u'面积':content[2]})
        content=cj.find('div',attrs={'class':'houseInfo'}).text.replace('\xa0', '').replace(' ', '').split('|')
        if content:
            info_dict.update({u'朝向':content[0]})
            info_dict.update({u'装潢':content[1]})
            if len(content)>2:
                info_dict.update({u'设施':content[2]})
        info_dict.update({u'签约时间':cj.find('div',attrs={'class':'dealDate'}).text})
        info_dict.update({u'签约总价':cj.find('div',attrs={'class':'totalPrice'}).text})
        info_dict.update({u'签约单价':cj.find('div',attrs={'class':'unitPrice'}).text})
        content=cj.find('div',attrs={'class':'positionInfo'}).text.split()
        if content:
            info_dict.update({u'楼层':content[0]})
            try:
                info_dict.update({u'建成':content[1][0:4]})
            except Exception as e:
                pass
        content=cj.find('span',attrs={'class':'dealHouseTxt'})
        if content:
            content=content.text.split()
            for c in content:
                    if c.find(u'满')!=-1:
                        info_dict.update({u'房产类型':c})
                    elif c.find(u'距')!=-1:
                        info_dict.update({u'地铁':c})
                    elif c.find(u'学')!=-1:
                        info_dict.update({u'学区':c})

        command=gen_chengjiao_insert_command(info_dict)
        db_cj.execute(command,1)
        
def xiaoqu_chengjiao_spider(db_cj,db_xq,xq_name=u"朝阳",url_base=chengjiaolink):
    """
    爬取小区成交记录
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
        #url_page=cj1 % (i+1,xq_name)
        #t=threading.Thread(target=chengjiao_spider,args=(db_cj,db_xq,url_page))
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
    #con=sqlite3.connect('sh-chengjiao.db')
    #sql="select * from chengjiao"
    #data=read_sql(sql,con)
    data=db_cj.fetchall(con)
    #for xq_name,val in zip(data.groupby(['district']).count()[['name']].index,data.groupby(['district']).count()['name'].values):
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
        #num=soup.find('div',attrs={'class':'total fl'}).span.text
        #total_pages=int(ceil(float(num)-val)/20)
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
    
    
    
#########异常处理



def a1(dataframe,col='',threshold=.95):
    d=dataframe[col]
    dataframe[col+'isAnomaly']= (d>d.quantile(threshold)) | (d< d.quantile(1-threshold))
    return(dataframe)



def a2(dataframe,col='',threshold=3.5):

    d=dataframe[col]
    zscore=(d-d.mean())/d.std()
    dataframe[col+'isAnomaly']= (zscore.abs()> threshold) | (zscore.abs()< -threshold)
    return(dataframe)

def a3(dataframe,col='',threshold=3.5):

    dd=dataframe[col]
    MAD=(dd -dd.median()).abs().median()
    zscore=((dd -dd.median())*0.6475 /MAD).abs()
    dataframe[col+'isAnomaly']= (zscore > threshold) | (zscore <- threshold)
    return(dataframe)



########################################################################

if __name__=="__main__":
    #[u'小区名称',u'大区域',u'小区域',u'户型',u'结构',u'建造时间',u'挂牌均价',u'在售',u'成交',u'出租',u'地铁']
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, style TEXT, jieogu Text,year TEXT,price TEXT,sell TEXT,chengjiao TEXT, chuzu TEXT, school TEXT ,subway TEXT)"   
    db_xq=SQLiteWraper('bj-xiaoqu.db',command)
    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT,district TEXT, road TEXT, orientation TEXT ,zhuanghuang TEXT, floor TEXT, sheshi TEXT,year TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT)"  
    db_cj=SQLiteWraper('bj-chengjiao.db',command)
    
    for region in regionsb:
        do_xiaoqu_spider(db_xq,region)
        
    do_xiaoqu_chengjiao_spider(db_cj,db_xq,command='select distinct regions from xiaoqu')
    
    exception_spider(db_cj,db_xq)