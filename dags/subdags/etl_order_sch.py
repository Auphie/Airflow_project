import pandas as pd
import numpy as np
import json
from functools import reduce
from bridge import MySql

sql_ord="""select *** from *** where ***"""

sql_kid="""select *** from *** where ***"""

# 2020-11-9 adding '***' & `***` when *** has been abandoned
sql_cancel=""" select *** from *** where ***"""

sql_affi="""select *** from *** where ***"""

sql_host="""select *** from *** where ***"""

sql_credit="""select *** from *** where ***"""

sql_exc="""select *** from *** where ***"""

dml_sql="""{method} from {table_name} where *** >= curdate()
 or date(***) > ***"""

def exchangeRate(data):
    result = []
    for item in data:
        rateJson = json.loads(item[3])
        pay_key = item[1]
        data={}
        data['***'] = item[0]
        data['***'] = 1.0 if pay_key == 'TWD' else rateJson[pay_key].get('TWD')
        try:
            data['***'] = json.loads(item[2]).get('names')['en-us']
            data['***'] = json.loads(item[2]).get('names')['zh-tw']
        except (TypeError, ValueError):
            data['***'] = '-'
            data['***'] = '-'
        result.append(data)
    df_exc = pd.DataFrame(result)
    return df_exc


if __name__ == '__main__':
    conn_id='auphie-mysql'
    df_ord   = MySql.read_mySQL(sql_ord,conn_id)
    df_kid   = MySql.read_mySQL(sql_kid,conn_id)
    df_cancel= MySql.read_mySQL(sql_cancel,conn_id)
    df_affi  = MySql.read_mySQL(sql_affi,conn_id)
    df_exc   = exchangeRate(MySql.read_rawMySQL(sql_exc,conn_id))
    df_host  = MySql.read_mySQL(sql_host,conn_id)
    df_credit= MySql.read_mySQL(sql_credit,conn_id)
    dfs = [df_ord, df_kid, df_cancel, df_affi, df_host, df_credit, df_exc]
    df = reduce(lambda left,right: pd.merge(left,right,on='***', how='left'), dfs)
    df['***']=df['***'].astype(float)
    df['***']= round(df['***'] * df['***'],0)
    df['***']=df['***'].replace('',0).replace(np.nan,0)
    df['***']=df['***'].replace('',0).replace(np.nan,0)
    df['***']=df['***'].replace('',np.nan)
    df['***']=df['***'].replace('',np.nan)
    df['***'] = df['***'].str.extractall(r'入住時間約\d+:\d+&amp;lt;br&amp;gt;(.*)')  \
                          .unstack().replace(np.nan,' ').apply('; '.join, 1)
    df['***'] = np.where(df['***'].isnull(), 'N', 'Y')
    df['***'] = np.where(df['***'].isnull(),'***',df['***'])
    df['***'] = df['***'].replace(',','，')

    df.name = '***'
    MySql.dml_DWH('delete', df.name, dml_sql)
    MySql.write_DWH(df,'append')
