from bridge import BigQuery as bq
from bridge import MySql
import pandas as pd
import copy

sql1="""SELECT `Date`, Page, Pageviews, uniquePageviews
        from ***.***
        where Page like '%aff_id=%'
          and date(Date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)"""
#          and date(Date) between date('2020-11-04') and date('2020-11-11')"""

sql2="""select *** from ***;"""

sql3="""select distinct Date from ***"""

if __name__ == '__main__':
    df1 = bq.read_gbq(sql1)
    df1 = df1[~df1['Page'].str.contains(r'/?pre=on')]
    df1['Date'] = df1['Date'].dt.date
    df1['***'] = df1['Page'].str.extract(r'/(\d{2,7})/').fillna('0').astype(int)
    df1['***'] = df1['Page'].str.extract(r'/([A-Z|a-z]{2}-[A-Z|a-z]{2})/view/')
    df1['people'] = df1['Page'].str.extract(r'people=(\d+)')
    df1['***'] = df1['Page'].str.extract(r'check_in_date=(\d{4}-\d{2}-\d{2})')
    df1['***'] = df1['Page'].str.extract(r'check_out_date=(\d{4}-\d{2}-\d{2})')
    df1['***'] = pd.to_datetime(df1['check-in'], format='%Y-%m-%d', errors='coerce').dt.date
    df1['***'] = pd.to_datetime(df1['check-out'], format='%Y-%m-%d', errors='coerce').dt.date
    df1['***'] = df1['Page'].str.extract(r'aff_id=(\d+)')

    conn_2='auphie-mysql'
    df2 = MySql.read_mySQL(sql2,conn_2)

    df = pd.merge(df1, df2, how='inner', on='bnb_id')
    df.name = '***'

    insert_date = list(pd.to_datetime(df.Date).apply(lambda x: x.date()))
    conn_3='dwh_mysql'
    hist_df = MySql.read_mySQL(sql3,conn_3)
    hist_date = hist_df.Date.unique()
    if set(insert_date).issubset(hist_date):
        print("Insert DWH error, duplicate data by same date insert!")
    else:
        MySql.write_DWH(df,'append')
