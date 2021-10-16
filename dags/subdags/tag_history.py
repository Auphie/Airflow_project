import pandas as pd
from bridge import BigQuery as bq
from bridge import MySql

sql1="""select ***,
  trim(SUBSTRING_INDEX(SUBSTRING_INDEX(z.***, ',', numbers.n), ',', -1)) 'tag' from
  (
    SELECT @row := @row + 1 as n FROM
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4) t,
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4) t2,
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4) t3,
    (SELECT @row:=0) r
  ) numbers JOIN (select ***
                from ***
                where ***) as z
  on CHAR_LENGTH(z.***)
     -CHAR_LENGTH(REPLACE(z.***, ',', ''))>=numbers.n-1"""

sql2="""select *** from ***"""


if __name__ == '__main__':
    conn_1='dwh_mysql'
    df1 = MySql.read_mySQL(sql1,conn_1)
    df2 = MySql.read_mySQL(sql2,conn_1)
    df = pd.merge(df1, df2, how='inner', on='bnb_id')
    df.name='***'
    insert_date = (pd.to_datetime(df.t_date).apply(lambda x: x.date())).unique()

    sql_bq="""select *** from (
    SELECT *** FROM ***.%s group by *** order by 1 desc
    ) as z"""%(df.name)
    hist_df = bq.read_gbq(sql_bq)
    hist_date = hist_df.t_date.dt.date.unique()

    if set(insert_date).issubset(hist_date):
        print("Insert DWH error, duplicate data by same date insert!")
    else: bq.write_gbq(df,'append')