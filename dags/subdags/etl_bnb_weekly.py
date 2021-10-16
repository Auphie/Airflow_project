import pandas as pd
from bridge import MySql

sql0="""select * from DW_table_name1"""

sql1="""select *, 'new_open' as `operation`
 from ***
 where date(rec_date) = curdate() - INTERVAL DAYOFWEEK(curdate()) -1 DAY
  and (_id) not in (select _id
      from ***
      where date(rec_date) <= curdate() - INTERVAL DAYOFWEEK(curdate()) +6 DAY
      )
  and (_id) in (select _id from *** where ***)
 union
 select ***, 'Reopen' as `operation`
 from ***
 where date(rec_date)= curdate() - INTERVAL DAYOFWEEK(curdate()) -1 DAY
  and (_id) not in (select _id from ***
      where date(rec_date) = curdate() - INTERVAL DAYOFWEEK(curdate()) +6 DAY
      )
  and (_id) in (select _id from ***
      where date(rec_date) < curdate() - INTERVAL DAYOFWEEK(curdate()) +6 DAY
      )
  and (_id) in (select _id from *** where ***)
union
 select pkey, timestamp(now() - INTERVAL DAYOFWEEK(curdate()) -1 DAY) `rec_date`,
   ***, 'close' as `operation`
 from ***
 where date(rec_date)= curdate() - INTERVAL DAYOFWEEK(curdate()) +6 DAY
  and (_id) not in (select id from ***
      where date(rec_date) = curdate() - INTERVAL DAYOFWEEK(curdate()) -1 DAY
      )
  and (_id) in (select _id from *** where ***)"""

sql2="""select ***
from *** as s
join (
    SELECT ***
    FROM ***
    where *** between timestamp(curdate(),'06:00:00') - INTERVAL DAYOFWEEK(curdate()) +6 DAY
  and timestamp(curdate(),'06:00:00') - INTERVAL DAYOFWEEK(curdate()) -1 DAY    group by _id
) as z on s.id = z.id
join ***"""

sql3="""select ***
      from (select _id, *** from ***
	    where date(***)= curdate() - INTERVAL DAYOFWEEK(curdate()) -1 DAY
           ) as h1,
           (select _id, *** from ***
	    where date(***) = curdate() - INTERVAL DAYOFWEEK(curdate()) +6 DAY
	   ) as h2
      where ***"""

sql4="""select ***
 from (select _id, ***
          where date(r***)= curdate() - INTERVAL DAYOFWEEK(curdate()) -1 DAY
      ) as h1,
      (select _id, *** from ***
          where date(***) = curdate() - INTERVAL DAYOFWEEK(curdate()) +6 DAY
        ) as h2
 where ***"""


if __name__ == '__main__':
    conn_1='dwh_mysql'
    conn_2='mysql'

    df0 = MySql.read_mySQL(sql0,conn_1)
    df1 = MySql.read_mySQL(sql1,conn_1)
    df2 = MySql.read_mySQL(sql2,conn_2)
    df_weekly = pd.merge(df1, df2, how='left', on='_id')
    df_weekly = pd.merge(df_weekly, df0[['_id','***']], how='inner', on='_id')
    df_weekly.name = '***'
    MySql.write_DWH(df_weekly,'replace',alt_script)

    df3 = MySql.read_mySQL(sql3,conn_1)
    df4 = MySql.read_mySQL(sql4,conn_1)
    df_change = pd.merge(df3, df4, how='outer', on='_id').fillna('N')
    df_change = pd.merge(df_change, df_weekly[['_id','***']], how='left', on='_id').fillna('unchanged')
    df_change = pd.merge(df_change, df0, how='inner', on='_id')

    df_change.name = '***'
    MySql.write_DWH(df_change,'replace',alt_script2)
