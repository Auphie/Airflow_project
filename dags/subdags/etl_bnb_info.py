import pandas as pd
import numpy as np
from bridge import MySql

sql1="""select bnb_id,
***
    GROUP_concat(air_url SEPARATOR '\n') as air_url,
	count(xxx) 'room_types',
	sum(xxx) 'tot_rooms',
	sum(xxx) 'de_rooms',
    sum(xxx) 'adj_rooms',
    sum(xxx) 'cap_rooms'
from table_name
group by xxx"""

sql2="""select xxx, group_concat(z.xxx SEPARATOR ', ') 'tags'
 from ( select xxx
	 from xxx as t
	 join xxx
	 join xxx
	 where xxx) as z
group by xxx"""

dml_sql="""{method} TABLE {table_name}"""


if __name__ == '__main__':
    conn_1='dwh_mysql'
    df1=MySql.read_mySQL(sql1,conn_1)
    df1['xxx'] = pd.to_datetime(df1['xxx']).dt.time
    df1['xxx'].map({1: 'Y', 0: 'N'}).fillna('unknown')
    df1['xxx']=np.where((df1['tot_rooms']-df1['de_rooms']<=0),'off',df1['online'])

    conn_2='auphie-mysql'
    df2=MySql.read_mySQL(sql2,conn_2)

    df = pd.merge(df1, df2, how='left', on='xxx')
    df.name='DW_table_name'
    MySql.dml_DWH('TRUNCATE', df.name, dml_sql)
    MySql.write_DWH(df,'append')
