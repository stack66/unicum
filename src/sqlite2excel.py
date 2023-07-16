#-------------------------------------------------------------------------------
# Name:        un
# Purpose:
#
# Author:      manager
#
# Created:     15.10.2022
# Copyright:   (c) manager 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import sys
import pygsheets
import json
#from openpyxl import Workbook
#from openpyxl import load_workbook
#from openpyxl.worksheet.table import Table

if sys.platform=='win32':
    dbn = 'unic-en.db'
    sfpath ='F:/Share/yahoo-finance-357713-23251f85fccf.json'
else:
    dbn = '/usr/local/share/unic-en.db'
    sfpath ='/usr/local/share/yahoo-finance-357713-23251f85fccf.json'

ta = 'ta'
ogp = 'ogp'
tabase = 'tabase'
inc_daily = 'inc_daily'
temp = 'temp'
work = 'work02'


kwork = 'https://docs.google.com/spreadsheets/d/1uHa1IxoT6RDJ0VwcrHPFX2MwpNAkPW08V8ydNB9KhBs/edit#gid=0'
kwork1 ='https://docs.google.com/spreadsheets/d/1ctozGWRENnUIbQKNZ7ln-udcHiTS_t6OtJCnTy5JCGM/edit?usp=sharing'
wbn = '0002.xlsx'
def write_to_gsheet(service_file_path, spreadsheet_id, sheet_name, data_df, update=False):
    """
    """
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        wks = sh.worksheet_by_title(sheet_name)
    except:
        try:
            wks = sh.add_worksheet(sheet_name)
        except:
            pass
    wks_list = sh.worksheets()
    if update:
        wks.clear()
        wks.set_dataframe(data_df, (1,1), copy_index=False, encoding='utf-8', fit=True)
    else:   # append
        rows = wks.rows
        wks.set_dataframe(data_df, (1+rows,1), copy_index=False, encoding='utf-8', fit=True)

def write_to_ex(wbname=None, sheet=None, data=None):
    wb = Workbook()
    if sheet==None:
        ws = wb.active
    else:
        ws = wb.create_sheet(sheet)
    for row in data:
        ws.append(row)
    #tab = Table(displayName="Table1", ref="A1:E5")
    if wbname:
        wb.save(wbname)
    else:
        wb.save(wbn)

def main():
    '''
    conn = sqlite3.connect(dbn)
    c = conn.cursor()
    st = f'pragma table_info({ogp})'
    c.execute(st)
    rows = c.fetchall()
    conn.close()
    colnames = []
    for r in rows:
        colnames.append(r[1])
    '''
    engine = create_engine(f"sqlite:///{dbn}")
    with engine.connect() as conn, conn.begin():
        # WHERE Clause for next time diapasone
        # lastdate = 1672491600 
##        wclause = f'where tdate>{lastdate}'
##        queryi = f'select id,tdate from {temp} {wclause}'
##        queryq = f'select quantity from {temp} {wclause}'
##        queryc = f'select coin from {temp} {wclause}'
##        queryl = f'select left from {temp} {wclause}'

        # ------------------
        whclause = 'WHERE tdate>1667998800' # 09.11.22 23:00
        whcl2023 = 'WHERE tdate>1672491600' # 31.12.2022 23:00
        dist = 'DISTINCT'
        limit = 'LIMIT 200'
        queryta = f'select id,addr,tel from tabase'
        dfta = pd.read_sql(queryta, conn)
        queryi = f'SELECT id,tdate FROM {work} {whcl2023} ORDER BY tdate DESC'
        #queryi = f'SELECT id, DATETIME(tdate, 'auto') dt FROM {temp} ORDER BY tdate DESC'
        queryq = f'SELECT quantity FROM {work} {whcl2023} ORDER BY tdate DESC'
        queryc = f'SELECT coin FROM {work} {whcl2023} ORDER BY tdate DESC'
        queryl = f'SELECT left FROM {work} {whcl2023} ORDER BY tdate DESC'
        dfq = pd.read_sql(queryq, conn)
        dfc = pd.read_sql(queryc, conn)
        dfl = pd.read_sql(queryl, conn)
        dfids = pd.read_sql(queryi,conn)

        dfids['tdate'] = dfids['tdate'].apply(lambda _: str(pd.Timestamp.fromtimestamp(_,tz='Asia/Vladivostok'))[0:16])
    # concat dframes into new dfs according worksheets
    # quantity
    dfql = dfq.loc[0:,'quantity']
    # to json array string
    dfqs = ','.join(dfql)
    dfqs = '['+dfqs+']'
    newdfq = pd.read_json(dfqs, orient='records').fillna(0)
    # coin
    dfcl = dfc.loc[0:,'coin']
    # to json array string
    dfcs = ','.join(dfcl)
    dfcs = '['+dfcs+']'
    newdfc = pd.read_json(dfcs, orient='records').fillna(0.0)
    # leftover
    dfll = dfl.loc[0:,'left']
    # to json array string
    dfls = ','.join(dfll).replace('NaN','0').replace('\xa0','').replace('nan','0')
    dfls = '['+dfls+']'
    newdfl = pd.read_json(dfls, orient='records').fillna(0.0)

    joindfq = pd.concat([dfids,newdfq], axis=1)
    joindfc = pd.concat([dfids,newdfc], axis=1)
    joindfl = pd.concat([dfids,newdfl], axis=1)
    #joindfq = joindfq.rename({'id':'Авто №','tdate':'Дата'}, axis=1)
    #joindfc = joindfc.rename({'id':'Авто №','tdate':'Дата'}, axis=1)
    #joindfl = joindfl.rename({'id':'Авто №','tdate':'Дата'}, axis=1)


    spid = kwork.split('/')[5]
    #write_to_gsheet(sfpath, spid, 'Список автоматов', dfta,True)
    write_to_gsheet(sfpath, spid, 'Расход ингредиентов', joindfl,True)
    write_to_gsheet(sfpath, spid, 'Покупки', joindfq,True)
    write_to_gsheet(sfpath, spid, 'Суммы', joindfc,True)


    if conn is not None:
        conn.close()

if __name__ == '__main__':
    main()
