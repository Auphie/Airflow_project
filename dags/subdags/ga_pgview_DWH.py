import pandas as pd
import copy
from bridge import BigQuery as bq
from bridge import MySql

sql1="""SELECT `Date`, Page, Pageviews, uniquePageviews
        from ***.***
        where date(Date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)"""

sql2="""select *** from ***;"""

sql3="""select distinct Date from ***"""

if __name__ == '__main__':
    df = bq.read_gbq(sql1)
    df = df[df['Page'].str.contains(r'/***/')]
    df = df[~df['Page'].str.contains(r'/?pre=on')]
    df1 = copy.deepcopy(df)
    df1['Date'] = df1['Date'].dt.date
    df1['***'] = df1['Page'].str.extract(r'/(\d{2,7})/').fillna('0').astype(int)
    df1['***'] = df1['Page'].str.extract(r'/([A-Z|a-z]{2}-[A-Z|a-z]{2})/view/')
    df11 = df1.groupby(['***']).sum()[['***']].reset_index()

    conn_2='auphie-mysql'
    df2 = MySql.read_mySQL(sql2,conn_2)

    df_ins = pd.merge(df11, df2, how='inner', on='bnb_id')
    df_ins.name = '***'

    insert_date = list(pd.to_datetime(df_ins.Date).apply(lambda x: x.date()))

    conn_3='dwh_mysql'
    hist_df = MySql.read_mySQL(sql3,conn_3)
    hist_date = hist_df.Date.unique()
    if set(insert_date).issubset(hist_date):
        print("Insert DWH error, duplicate data by same date insert!")
    else:
        MySql.write_DWH(df_ins,'append')
