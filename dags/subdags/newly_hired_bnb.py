import pandas as pd
from bridge import Gsheet
from bridge import MySql

sql1="""select *** from ***"""

sql2="""select *** from *** where ***"""

dml_sql="""{method} TABLE {table_name}"""

if __name__ == '__main__':

    conn_1='dwh_mysql'
    df1=MySql.read_mySQL(sql1,conn_1)
    df2=MySql.read_mySQL(sql2,conn_1)

    sheet_url = 'G_sheet_url'
    tab_name = 'tab_name!A1:G'
    df3 = Gsheet.get2df(sheet_url, tab_name)
    df3.rename(columns={'Month':'open_date',
			'***':'***'},
			inplace=True)
    df3['***']=df1['***'].astype(int)
    df3['***_date']=pd.to_datetime(df3.***_date, format='%Y/%m', errors='coerce').dt.date
    df3=df3[['***']]
    dfA = pd.concat([df2, df3])
    df = dfA.merge(df1, on='bnb_id', how='inner')
    df.name='***'
    MySql.dml_DWH('truncate',df.name,dml_sql)
    MySql.write_DWH(df,'append')
