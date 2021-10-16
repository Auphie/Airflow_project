import pandas as pd
import json
from bridge import MySql

sql_exc = """select *** from *** where ***"""

dml_sql="""{method} TABLE {table_name}"""

if __name__ == '__main__':
    conn_id='auphie-mysql'
    exRate  = MySql.read_rawMySQL(sql_exc,conn_id)

    result = []
    for item in exRate:
        rateJson = json.loads(item[3])
        pay_key = item[1]
        data={}
        data['***'] = item[0]
        data['***'] = pay_key
        data['***'] = 1.0 if pay_key == 'TWD' else rateJson[pay_key].get('TWD')
#        data['***'] = pd.to_numeric(data['***'])
        try:
            data['***'] = json.loads(item[2]).get('names')['zh-tw']
            data['***'] = json.loads(item[2]).get('names')['en-us']
        except (TypeError, ValueError):
            data['***'] = '-'
            data['***'] = '-'
        result.append(data)

    df =pd.DataFrame(result)
    df.name = '***'
    MySql.dml_DWH('TRUNCATE', df.name, dml_sql)
    MySql.write_DWH(df,'append')
