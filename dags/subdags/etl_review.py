import pandas as pd
from bridge import MySql

sql1="""select *** from *** where ***"""

sql2="""select *** from ***"""

if __name__ == '__main__':
    conn_id1='auphie-mysql'
    conn_id2='dwh_mysql'
    df1 = MySql.read_mySQL(sql1,conn_id1)
    df2 = MySql.read_mySQL(sql2,conn_id2)
    review = pd.merge(df1, df2, how='left', on='***')
    review.name='***'
    MySql.write_DWH(review,'replace',alt_script)
