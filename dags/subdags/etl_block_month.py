import pandas as pd
import numpy as np
from bridge import MySql
from functools import reduce

sql1="""SQL_statement_1"""

sql2="""select ***
 from ( select ***
	  from {table} as sch
	  join ***
	  join ***
 	where ***
   and date(date) between curdate() and last_day(DATE_ADD(curdate(), INTERVAL {months} MONTH))
   ) as z
group by ***"""

dml_sql="""{method} TABLE {table_name}"""


if __name__ == '__main__':
    conn_1='dwh_mysql'
    conn_2='auphie-mysql'
    table_w = 'DB_table_name'
    params = [[1,'DB_table_name1'],
            [2,'DB_table_name2']],
            ...
            ]

    MySql.dml_DWH('truncate',table_w,dml_sql)

    for i in range(0,len(params)):
        country_id = params[i][0]
        table = params[i][1]
        months = 9

        df1 = MySql.read_mySQL(sql1,conn_1)
        df2 = MySql.read_mySQL(sql2.format(table=table,c_id=country_id,months=months),conn_2)
        df = pd.merge(df2,df1,on=['bnb_id','room_id'], how='inner')
        df.name = table_w
        MySql.write_DWH(df,'append')
#        for key, group in df.groupby('month'):
#            group.name=table_w
#            MySql.write_DWH(group,'append')
