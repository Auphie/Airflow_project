import pandas as pd
import numpy as np
from bridge import MySql
from functools import reduce

sql1="""select ***
from *** where ***"""

sql2="""select ***
  from {table} as sch
  join ***
   and *** = {c_id}
 where sch.***
   and date(sch.***) between curdate() and last_day(DATE_ADD(curdate(), INTERVAL {months} MONTH))
group by ***"""

dml_sql="""{method} TABLE {table_name}"""


if __name__ == '__main__':
    conn_1='dwh_mysql'
    conn_2='auphie-mysql'

    table_w = '***'
    params = [[1,'***'],
            [2,'***'],
            [6,'***'],
            [5,'***'],
            [8,'***'],
            [10,'***'],
            [13,'***']]

    MySql.dml_DWH('truncate',table_w,dml_sql)

    for i in range(0,len(params)):
        *** = params[i][0]
        table = params[i][1]
        months = 9
        df1 = MySql.read_mySQL(sql1,conn_1)
        df2 = MySql.read_mySQL(sql2.format(table=table,c_id=country_id,months=months),conn_2)
        df = pd.merge(df2,df1,on=['***'], how='inner')
        df['avl_rooms'] = df['tot_rooms'] - df['block_num'] - df['***']
        df['avl_rooms'] = np.where(df.avl_rooms > 0, df.avl_rooms, 0)
        df['avl_rooms'] = np.where(df.avl_rooms > df.cap_rooms, df.cap_rooms, df.avl_rooms)
        df = df.drop(columns=['***'])
        df.name = table_w
        MySql.write_DWH(df,'append')
#        for key, group in df.groupby('month'):
#            group.name=table_w
#            MySql.write_DWH(group,'append')
