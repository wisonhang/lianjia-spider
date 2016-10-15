
# coding: utf-8
"""
@author: wisonhang
@site: hangdawei@126.com
"""
import plotly.plotly as py
import scipy as sp
from numpy import *
from pandas import *
import sqlite3
import matplotlib.pyplot as plt # side-stepping mpl backend
import matplotlib.gridspec as gridspec # subplots
from plotly.graph_objs import *
import plotly.tools as tls
import matplotlib as mpl
import seaborn as sns
sns.set_style('darkgrid')
mpl.rcParams['font.sans-serif'] = ['SimHei'] #指定默认字体  
mpl.rcParams['axes.unicode_minus'] = False 
sns.set_context("talk")

def fangjia_plot(db='sh-chengjiao.db',sql="select * from chengjiao",group='district',ytime=201400):
    con=sqlite3.connect(db)
    data=read_sql(sql,con)
    testdata=data[['name','style','area','district','road','sign_time','unit_price','total_price']]
    testdata=testdata[Series(['元'in i for i in testdata['unit_price']])].copy()
    testdata[['area']]=testdata[['area']].applymap(lambda x : float (x[:x.index('平')]))
    testdata[['unit_price']]=testdata[['unit_price']].applymap(lambda x : float(x[:x.index('元')]))
    testdata[['total_price']]=testdata[['total_price']].applymap(lambda x : float(x[:x.index('万')]))
    testdata[['年月']]=testdata[['sign_time']].applymap(lambda x : int(x[0:7].replace('.','').replace('-','')))
    testdata=testdata.query('total_price>10').query('area>5').query('area<1500').query('年月>%s'%(ytime)).copy()
    testdata[['年月']]=testdata[['年月']].applymap(lambda x : str(x))
    district=list(testdata[group].unique())
    tt=list(sort(testdata.年月.unique()))
    area_av=DataFrame(index=tt)
    up_av=DataFrame(index=tt)
    up1_av=DataFrame(index=tt)
    tp_av=DataFrame(index=tt)
    sell_count=DataFrame(index=tt)
    for a in district:
        tdata=testdata[testdata[group]==a]
        summ=tdata.groupby('年月').mean().applymap(lambda x : round(x,2))
        allsum=tdata.groupby('年月').sum()
        summ=summ.join(DataFrame((allsum['total_price']/allsum['area'])*10000,columns=['unit_price1']))
        count=tdata.groupby('年月').count()['name']
        area_av=area_av.join(summ['area'],rsuffix='new')
        up_av=up_av.join(summ['unit_price'],rsuffix='new')
        up1_av=up1_av.join(summ['unit_price1'],rsuffix='new')
        tp_av=tp_av.join(summ['total_price'],rsuffix='new')
        sell_count=sell_count.join(count,rsuffix='new')
    area_av.columns=up_av.columns=up1_av.columns=tp_av.columns=sell_count.columns=district
    '''
    plotdata={
    '大小':area_av.to_dict(),
    '均价0':up_av.to_dict(),
    '均价1':up1_av.to_dict(),
    '总价':tp_av.to_dict(),
    '数量':sell_count.to_dict()    
    }
    '''
    plotdata={
    '成交面积均值':area_av.to_dict(),
    '成交单价均值':up_av.to_dict(),
    '平均成交单价':up1_av.to_dict(),
    '成交总价均值':tp_av.to_dict(),
    '单月成交量':sell_count.to_dict()    
    }
    pp=Panel.from_dict(plotdata,orient='minor')
    return(pp,testdata)
#######################################################################
'''
def sbplot(pp,col=4,row=4,title='上海',fontsize=10,figsize=(15,10)):
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.style.use('ggplot')
    fig,ax=plt.subplots(col,row,figsize=figsize,sharex=True)
    for i in range(col):
        for j in range(row):
            try:
                name=pp.items[i*col+j]
            except Exception as e:
                pass
        #print(name)
            df=pp[name].fillna(0)
            #df[['均价0']].plot(secondary_y=True,ax=ax[i,j],style='g-',legend=True,linewidth=2.0)
            #df[['年月']]=df[['年月']].applymap(lambda x : str(x))
            df[['均价1']].plot(secondary_y=True,ax=ax[i,j],style='r-',legend=True,linewidth=2.0)
            df[['大小','总价','数量']].plot.bar(ax=ax[i,j], alpha=0.7)
            l1=lines =ax[i,j].right_ax.get_lines()
            l2=ax[i,j].get_legend_handles_labels()
            ax[i,j].legend(l1+l2[0], [l1[0].get_label()]+l2[1],loc='upper left',fontsize=fontsize)
            ax[i,j].set_ylabel('大小/m2,总价/万',fontsize=fontsize)
            ax[i,j].right_ax.set_ylabel('均价 万/m2',fontsize=fontsize)
            ax[i,j].set_xlabel('时间',fontsize=fontsize)
            ax[i,j].set_title(name+'二手房价格',fontsize=1.2*fontsize)
            ax[i,j].set_xlabel
            for label in ax[i,j].get_xticklabels() +ax[i,j].get_ymajorticklabels()+ax[i,j].right_ax.get_ymajorticklabels():
                label.set_fontsize(fontsize)
            #plt.subplots_adjust(wspace=wspace,hspace=hspace)
            #fig.set_figheight(figsize[0])
            #fig.set_figwidth(figsize[1])
    plt.suptitle(title+'二手房成交走势',fontsize=1.5*fontsize)
    #plt.tight_layout()
    plt.subplots_adjust(top=.95,wspace=.25,hspace=.25,bottom=.1,right=0.95,left=0.05)
    plt.show()
    return(fig)

'''

###################################################################################################

def sbplot(pp,col=3,title='上海',fontsize=10):
    sns.set_style('darkgrid')
    mpl.rcParams['font.sans-serif'] = ['SimHei'] #指定默认字体  
    mpl.rcParams['axes.unicode_minus'] = False 
    sns.set_context("talk")
    #mpl.style.use('ggplot')
    #sns.set_context(font_scale=font_scale)
    font = mpl.font_manager.FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=fontsize)
    mpl.rcParams['axes.titlesize']=1.5*fontsize
    mpl.rcParams['figure.titlesize']=2*fontsize
    num=len(pp.items)
    row=int(np.ceil(num/col))
    fig=plt.figure()
    for i in range(num):
        ax=fig.add_subplot(row,col,i+1)
        df=pp[pp.items[i]].fillna(0)
        #df[['平均成交单价','成交单价均值']]=df[['平均成交单价','成交单价均值']].applymap(lambda x : x/1000.0)
        df[['平均成交单价']].applymap(lambda x : x/10000.0).plot(secondary_y=True,ax=ax,style='r-',legend=False,linewidth=2.0,alpha=0.8)
        df[['成交面积均值','成交总价均值','单月成交量']].plot.bar(ax=ax, alpha=0.7)
        l1=lines =ax.right_ax.get_lines()
        l2=ax.get_legend_handles_labels()
        ax.legend(l1+l2[0], [l1[0].get_label()]+l2[1],loc='upper left',prop=font)
        ax.set_ylabel('大小/m2,总价/万',fontproperties=font)
        ax.right_ax.set_ylabel(u'均价 万/m2',fontproperties=font)
        ax.set_xlabel('时间',fontproperties=font)
        ax.set_title(pp.items[i]+u'二手房价格')
        for label in ax.get_xticklabels() +ax.get_yticklabels()+ax.right_ax.get_yticklabels():
            label.set_font_properties(font)
    plt.suptitle(title+u'二手房成交走势')
    plt.subplots_adjust(top=.9,wspace=.2,hspace=.15,bottom=.1,right=0.95,left=0.05)
    plt.show()


######################################################################
    
def snsplot(group='闸北',by='街道',data=None,col_wap=3,style='lm',title='杭州',xlim=[0,200],ylim=[0,2500],font_scale=1):
    sns.set_style('darkgrid')
    mpl.rcParams['font.sans-serif'] = ['SimHei'] #指定默认字体  
    mpl.rcParams['axes.unicode_minus'] = False 
    sns.set_context("talk",font_scale=font_scale)
    #set(fontsize=fontsize)
    if by=='街道':
        data=data[data['district']==group]
    data=data[['area','district','road','total_price','年月']].sort_values('年月')
    data.columns=['面积/m2','区域','街道','总价/万','年月']
    if style =='Facet':
        g=sns.FacetGrid(data,col=by,hue="年月",palette="husl",col_wrap=col_wap,legend_out=True,
                   xlim=xlim,ylim=ylim)
        g.fig.suptitle(title+'二手房成交') #此处需要先加title 再做变换
        g=(g.map(plt.scatter, "面积/m2","总价/万").set(xlim=xlim, ylim=ylim).add_legend()
         .fig.subplots_adjust(top=.9,wspace=.05,hspace=.15,bottom=.1,right=0.95,left=0.05))    
    elif style =='lm':
        g=sns.lmplot(x="面积/m2", y="总价/万", hue="年月", data=data,col='区域',
           col_wrap=col_wap, ci=None,palette="husl",scatter_kws={"s": 50, "alpha": 1})
        g.fig.suptitle(title+'二手房成交散点图')
        g = (g.set_axis_labels("面积/m2", "总价/万")
            .set(xlim=xlim, ylim=ylim).add_legend()
            .fig.subplots_adjust(top=.9,wspace=.05,hspace=.15,bottom=.1,right=0.95,left=0.05))
    #plt.tight_layout()
    return(g)

##################################################################################################
def snsvio(by='区域',group='闸北',key=2,data=None,style='',col_wap=3,title='杭州',font_scale=1,fontsize=15,ylim=[0,5],scale='count'):
    sns.set_style('darkgrid')
    mpl.rcParams['font.sans-serif'] = ['SimHei'] #指定默认字体  
    mpl.rcParams['axes.unicode_minus'] = False 
    sns.set_context("talk",font_scale=font_scale)
    mpl.rcParams['axes.labelsize']=1.5*fontsize
    mpl.rcParams['axes.titlesize']=2*fontsize
    rowname=['成交面积/m2','成交总价/万','成交单价\n万元/平']
    #set(fontsize=fontsize)   
    if by=='街道':
        data=data[data['district']==group]
    data=data[['area','total_price','unit_price','district','road','年月']].sort_values('年月')
    data[['unit_price']]=data[['unit_price']].applymap(lambda x: x/10000)
    data.columns=['面积','总价','单价','区域','街道','年月']
    if style=='vio':
        g=sns.violinplot(x=by, y=data.columns[key],data=data,scale=scale, inner="quartile")
        g.set_ylabel(rowname[key])
        g.set_title(title+'二手房'+data.columns[key]+'小提琴图')
        g.set(ylim=ylim)
        return(g)  
    g=sns.FacetGrid(data,col='年月',palette="husl",col_wrap=col_wap,legend_out=True,sharey=False)                   
    g.fig.suptitle(title+'二手房'+data.columns[key]+'小提琴图') #此处需要先加title 再做变换
    g=(g.map(sns.violinplot, by,data.columns[key],scale=scale,palette='husl').set_axis_labels(by,rowname[key])
    .set_titles("时间 {col_name}").set(ylim=ylim)
    .add_legend()
    .fig.subplots_adjust(top=.9,wspace=.15,hspace=.15,bottom=.1,right=0.95,left=0.1))
    return(g)

 if __name__=="__main__":    
        db='hz-chengjiao.db'
        sql="select * from chengjiao"
        pp,data=fangjia_plot(db,sql,group='district',ytime=201500)
        fig=sbplot(pp,col=3,title='杭州',fontsize=15)
        snsplot(by='区域',data=data,col_wap=3,style='lm',xlim=[0,200],ylim=[0,1000],font_scale=1.5)
        plt.show()
        snsplot(by='区域',data=data,col_wap=3,style='Facet',xlim=[0,200],ylim=[0,1000],font_scale=1.5)
        plt.show()
        snsvio(by='区域',data=data,col_wap=3,font_scale=1.5)
        plt.show()
