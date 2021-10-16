import pandas as pd
import numpy as np
import re
from bridge import MySql
from bridge import Gsheet
from bridge import Air

spreadsheet_id = 'G_sheet_id'

dml_sql="""{method} TABLE {table_name}"""

def tab2df(tab_name, tab_range, columns):
    df = Gsheet.get2df(spreadsheet_id, tab_range)
    df = df[[columns[0], columns[1]]]
    df.rename(columns={columns[0]:'***', columns[1]:'user_group'}, inplace=True)
    df = df[~df['***'].str.strip().replace('',np.nan).isna()]
    str(df['***']).replace(",","").replace(".00","").replace(".","")
    df['***']=columns[2]
    df['***']=tab_name
    df.reindex(columns=['tab_name', '***','***', '***'])
    return df

if __name__ == '__main__':

    tab_name = ['***']
    tab_range = ['***']
    columns = [['***'],
               ['***'],
               ['***'],
               ...]

    table_name = '***'
    MySql.dml_DWH('TRUNCATE', table_name, dml_sql)

    count=0
    for i in range(len(tab_name)):
        try:
            df = tab2df(tab_name[i], tab_range[i], columns[i])
            df.name = table_name
            MySql.write_DWH(df,'append')
            count+=1
        except KeyError as e:
            Air.writeLog(e)
#            continue

    if count < len(tab_name):
        print("update failure %d schedules"%(len(tab_name)-count))
        raise KeyError('incompleted')
