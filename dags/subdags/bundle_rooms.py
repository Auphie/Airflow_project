import pandas as pd
from bridge import Gsheet
from bridge import MySql

sql1="""select *** from *** where ***"""


if __name__ == '__main__':
    conn_1='dwh_mysql'
    df1=MySql.read_mySQL(sql1,conn_1)

    sheet_url = 'G_sheet_id'
    tab_name = 'tab_name!A1:E'
    df2 = Gsheet.get2df(sheet_url, tab_name)
    df2['***']='***'
    df2 = df2.replace('',0)
    df2['***']=df2['***'].astype(int)
    df2['***']=df2['***'].astype(int)

    df3 = df1[df1.bnb_id.isin(df2.bnb_id)]

#    df3 = df2[['***']].drop_duplicates().astype(int)
#    df4 = pd.merge(df1, df3, how='inner', on='***')
    df = pd.merge(df3,df2.drop(columns=['***']), how='left', on=['***'])

    df.name='***'
    MySql.write_DWH(df,'replace')