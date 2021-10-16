import pandas as pd
from bridge import MySql
from bridge import Gsheet

if __name__ == '__main__':
    spreadsheet_id = 'G_sheet_id'
    range_name = 'tab_name!A1:G'
    df = Gsheet.get2df(spreadsheet_id, range_name)
#    df['surfix2Bin']=df['surfix2Bin'].replace('', None)
    df['field_name']=df['field_name'].replace('', None)
    df.name = 'DB_table_name'
    dml_sql="""{method} TABLE {table_name}"""
    MySql.dml_DWH('TRUNCATE', df.name, dml_sql)
    MySql.write_DWH(df,'append')
