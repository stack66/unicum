#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      manager
#
# Created:     20.10.2022
# Copyright:   (c) manager 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import re
import requests
import datetime
from lxml import etree
import sqlite3
import sys
import pandas as pd
import json
if sys.platform=='win32':
    dbn = 'unic-en.db'
else:
    dbn = '/usr/local/share/unic-en.db'
from time import sleep
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 OPR/91.0.4516.65',
    "Connection": "keep-alive"
    }
tab = 'work02'
root = 'https://online.unicum.ru'
loginurl = 'https://online.unicum.ru/n/'
autolisturl = 'https://online.unicum.ru/n/vmcs.html'
incassurl = 'https://online.unicum.ru/n/incass_list.html?V'
incass_postfix = 'incass_list.html?V'
sgraphurl = 'https://online.unicum.ru/n/sgraph.html?V' # url анализ продаж
vgraphurl = 'https://online.unicum.ru/n/vgraph.html?V' # url
data = {
'httpauthreqtype': 'G',
'Login': 'user001',
'Password': '718293'
}
'''
    Route :
        1. login - loginurl
        2. ta list - autolisturl  - get hashes like "0300009E60" save into db
        3. select ta from list - goto
        -- variants
            a) Анализ продаж sgraphurl
            b) Текущий график продаж vgraphurl
            c) Инкассации incassurl - выбор последней инкассации
'''
def save_cookie(cookie=None):
    if cookie is not None:
        dnow=datetime.datetime.now()
        tstamp = str(round(dnow.timestamp()))
        with open(tstamp,'w') as f:
            f.write(cookie)

def get_cookie(cookie=None):
    return cookie

def reconnect():
    #get_cookie()
    session = requests.Session()
    session.headers.update(headers)
    response =  session.post(loginurl, data=data)   # Login or session ended


def main():
    session = requests.Session()
    session.headers.update(headers)
    try:
        response =  session.post(loginurl, data=data)   # Login or session ended
    except:
        with open('error.log','a') as f:
            dnow = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
            f.write(f'{dnow} : Error post request to {loginurl} script aborted\n')
            sys.exit('Script aborted')
    # assert response.status_code == requests.codes.ok
    html = response.text
    h = response.headers
    cookie = h['Set-Cookie']
    #save_cookie(cookie)
    # Test on after authorization page
    afterpage = etree.HTML(html)
    title = afterpage.xpath('//title')[0].text
    #assert 'Главная' in title
    try:
        response = session.get(autolisturl)
        sleep(1)
    except:
        with open('error.log','a') as f:
            dnow = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
            f.write(f'{dnow} : Error post request to {loginurl} script aborted\n')
            sys.exit('Script aborted')
    html = response.text
    afterpage = etree.HTML(html)
    title = afterpage.xpath('//title')[0].text
    #assert 'Торговые' in title     # Список автоматов
    links = afterpage.xpath('//table[5]/tr/td[2]/a/@href')
    names = afterpage.xpath('//table[5]/tr/td[2]/a/text()')
    names[:] = [el.replace('\xa0','') for el in names]
    uids = [el.split('?')[1] for el in links]
    phones = afterpage.xpath('//table[5]/tr/td[5]/p/text()')
    phones[:] = [el.replace('+','') for el in phones]
    adrs = afterpage.xpath('//table[5]/tr/td[3]/p/text()')
    stat = afterpage.xpath('//table[5]/tr/td[6]/p/font/text()')
    status = [False if 'OFF' in el else True for el in stat]
    ''' Write to SQLite "tabase"
    con = sqlite3.connect(dbn)
    c = con.cursor()
    rows = zip(names,uids,adrs,phones,status)
    c.executemany('insert or replace into tabase(id,hash,addr,tel,status) values(?,?,?,?,?)',rows)
    con.commit()
    con.close()
    '''
    con = sqlite3.connect(dbn)
    c = con.cursor()
    urls = []
    for u in uids:
        urls.append(vgraphurl+u) # Текущий график продаж
    for i, url in enumerate(urls):
        '''
        if not status[i]:
            continue    # Exclude OFFLINE
        '''
        try:
            r = session.get(url) #, timeout=(3,3))
            print(i,'->',url)
        except requests.exceptions.RequestException as err:
            if err == 10054:
                    #reconnect()
                    #r = session.get(url, timeout=(3,3))
                    dnow = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
                    with open('error.log','a') as f:
                        f.write(f'{dnow} : {err} {url} \n')
                        sys.exit('Connection abort')

        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
            #continue
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
            #continue
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            #continue
        sleep(1)
        ap = etree.HTML(r.text)
        #  Возможен вариант "За запрошенный период продаж не обнаружено"
        # проверить есть ли таблица
        istab = ap.xpath('//table')
        if istab:
            pass
        else:
        # Перключаемся по ссылке "Предыдущий период" - переход в последнюю инкассацию
            link = ap.xpath('//a/@href')[1]
            goto = root+link
            try:
                r = session.get(goto) #, timeout=(3,3))
            except requests.exceptions.RequestException as err:
                if err == 10054:
                    #reconnect()

                    dnow = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
                    with open('error.log','a') as f:
                        f.write(f'{dnow} : {err} {url} \n')
                        sys.exit('Connection abort')
            except requests.exceptions.HTTPError as errh:
                print ("Http Error:",errh)
                #continue
            except requests.exceptions.ConnectionError as errc:
                print ("Error Connecting:",errc)
                #continue
            except requests.exceptions.Timeout as errt:
                print ("Timeout Error:",errt)
                #continue
        try:
            df=pd.read_html(r.text)[0]
        except:
            continue
        tp = etree.HTML(r.text)
        try:
            ta = tp.xpath('//h1/a/text()')[0].strip()   # id auto
        except:
            dnow = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
            with open('error.log','a') as f:
                f.write(f'{dnow} : url {url} goto  {goto}\n')
            continue
        ncols = len(df.columns)             # кол. столбцов
        nrows = len(df.index)               # кол. строк
        if nrows < 10:    # wrong table
            dnow = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
            with open('error.log','a') as f:
                f.write(f'{dnow} -- Wrong table url {url} \n')
            continue
        df1=df.iloc[1:11,ncols-2].fillna('0/0') # NAN -> '0/0' !!! только первые 10 строк
        # quantity frame
        dfq = df1.apply(lambda x: x.split('/')[0].strip())

        #rowsumvectordfq = dfq.sum(axis=0)
        #colsumvectordfq = dfq.sum(axis=1)
         # cost frame
        dfc = df1.apply(lambda x: x.split('/')[1].strip())

        #rowsumvectordfc = dfc.sum(axis=0)
        #colsumvectordfc = dfc.sum(axis=1)
        leftover = df.iloc[11:nrows-2,ncols-2]
        #dates = df.iloc[0,1:ncols-2]
        tdate = df.iloc[0,ncols-3]
        tdate=tdate[:8]+'T'+tdate[8:]
        #dts = dates.apply(lambda x: x[:8]+'T'+x[8:])
        #dt = dts.apply(lambda x: int(pd.Timestamp(x,tz='Asia/Vladivostok').round(freq='T').timestamp())) # !!! timezone +7
        tmp = tdate.split('/')
        t0=tmp[0]
        t1=tmp[1]
        tmp[0]=t1
        tmp[1]=t0
        tdate=''.join(tmp)
        dt = int(pd.Timestamp(tdate,tz='Asia/Vladivostok').timestamp())
        ## список наименований !!! 2 вида наименований (формата таблиц)
        lname =[]
        for el in list(df.iloc[1:nrows-2,0]):
            lname.append(re.sub("\(.*?\)","",el))
        # Формируем JSON вида
        # {"name1": val1,...,"nameX": valXX } name = наименования - столбец 0
        jsq=''
        for i,v in dfq.items():
            jsq +=f'"{lname[i-1]}":{v},'
        jsq='{'+jsq[:-1]+'}'
        jsc = ''
        for i,v in dfc.items():
            jsc +=f'"{lname[i-1]}":{v},'
        jsc='{'+jsc[:-1]+'}'
        # Костыль - таблица другого вида
        if len(lname)<22:
            jtemp = '{" Кофе, мг":0," Молоко, мг":0," Шоколад, мг":0," Вода, мл":0," Стаканы, шт":0," Крышки, шт":0," Сахар, стики":0," Сироп, л":0," Мешалки, шт":0," Трубочки, шт":0," Капхолдеры, шт":0," Переноски, шт":0}'
            jj=json.loads(jtemp)
            jj[" Кофе, мг"]=float(leftover[12])
            jj[" Молоко, мг"]=float(leftover[13])
            jj[" Шоколад, мг"]=float(leftover[14])
            jj[" Вода, мл"]=float(leftover[11])
            jj[" Стаканы, шт"]=float(leftover[17])
            jj[" Крышки, шт"]=float(leftover[18])
            jj[" Сахар, стики"]=float(leftover[16])
            jj[" Сироп, л"]=float(leftover[15])
            jj[" Мешалки, шт"]=float(leftover[20])
            jj[" Трубочки, шт"]=float(leftover[19])
            left=json.dumps(jj,ensure_ascii=False)
        else:
            jleft=''
            for i, v in leftover.items():
                jleft += f'"{lname[i-1]}":{v},'
            left = '{'+jleft[:-1]+'}'
        query = f'insert into {tab}(id,tdate,quantity,coin,left) values(?,?,?,?,?)'
        zipped = (ta,dt,jsq,jsc,left)
        if len(jsq)>203:

            print(jsq)
            with open('badautos.txt','a') as f:
                line = f"{dt} {ta} {url} \n"
                f.write(line)
            continue
        c.execute(query,zipped)
        con.commit()

    if con is not None:
        con.close()


if __name__ == '__main__':
    main()
#    '''
#    Адрес: 680009, г. Хабаровск, ул. Промышленная, 20 (склад тест)
#    https://online.unicum.ru/n/vgraph.html?V030002AA53
#    ТТ21001392
#    File "F:\Temp\code\unic2r.py", line 198, in <lambda>
#        dfc = df1.apply(lambda x: x.split('/')[1].strip())
#    IndexError: list index out of range
#    '''