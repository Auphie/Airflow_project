import pandas as pd
import numpy as np
from bridge import MySql


sql1="""select ***,
  cast(SUBSTRING_INDEX(SUBSTRING_INDEX(z.content, ',', numbers.n), ',', -1) as unsigned) 'xxx'
from
  (
    SELECT @row := @row + 1 as n FROM
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6
 union all select 7 union all select 8 union all select 9) t,
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6
 union all select 7 union all select 8 union all select 9) t2,
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6
 union all select 7 union all select 8 union all select 9) t3,
    (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6
 union all select 7 union all select 8 union all select 9) t4,
    (SELECT @row:=0) r
  ) numbers
JOIN ( select ***
from ***
join ***
where ***
) as z
  on CHAR_LENGTH(z.xxx)
     -CHAR_LENGTH(REPLACE(z.xxx, ',', ''))>=numbers.n-1"""

sql2="""select *** from ***"""

sql3="""select *** from *** where ***"""

if __name__ == '__main__':
    conn_1 = 'auphie-mysql'
    conn_2 = 'dwh_mysql'
    df1 = MySql.read_mySQL(sql1,conn_1)
    df2 = MySql.read_mySQL(sql2,conn_2)
    df3 = MySql.read_mySQL(sql3,conn_2)

    dfA = df1.merge(df2, on='***', how='inner')
    dfA.name='***'
    MySql.write_DWH(dfA,'replace')

    dfA.drop(columns=['***'], axis=1, inplace=True)
    dfB = dfA.merge(df3, on='***', how='left')
    dfB.name='etl_***'
    MySql.write_DWH(dfB,'replace',alt_script)
