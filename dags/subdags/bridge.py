from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pandas_gbq
import traceback
from sqlalchemy import create_engine
from airflow.contrib.hooks import gcp_api_base_hook
from airflow.hooks.mysql_hook import MySqlHook
from google.oauth2.service_account import Credentials as a_path
from apiclient.discovery import build


class Air:
    def __init__(self, conn_id):
        self.root_folder = Path(__file__).parent.parent.parent
        self.gcp = gcp_api_base_hook.GoogleCloudBaseHook(gcp_conn_id=conn_id)
        self.SCOPES = [self.gcp._get_field('scope')]
        self.KEY_FILE = self.gcp._get_field('key_path')
        self.PROJECT_ID = self.gcp._get_field('project')

    @staticmethod
    def writeLog(err_msg):
        local_time = datetime.now()+ timedelta(hours=8)
        with open('errlog.txt', 'w+') as f:
            f.write("\nOccurence time(UTC+8): "+local_time.strftime("%Y-%m-%d %H:%M:%S")+'\n\n')
            f.write(str(err_msg))
#            f.write(traceback.format_exc())
        print('please read error in errlog.txt')


class MySql:

    def get_uri(hook):
        conn = hook.get_connection(getattr(hook, hook.conn_name_attr))
        login = ''
        if conn.login:
            login = '{conn.login}:{conn.password}@'.format(conn=conn)
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        charset = ''
        if conn.extra_dejson.get('charset', False):
            chrs = conn.extra_dejson["charset"]
            if chrs.lower() == 'utf8' or chrs.lower() == 'utf-8':
                charset = '?charset=utf8'
        return '{conn.conn_type}://{login}{host}/{conn.schema}{charset}'.format(
            conn=conn, login=login, host=host, charset=charset)

    def read_conn(conn_id):
        mysql = MySqlHook(mysql_conn_id=conn_id)
        con = mysql.get_conn()
        return con

    def write_conn():
        mysql = MySqlHook(mysql_conn_id='dwh_mysql')
        url = MySql.get_uri(mysql)
        engine = create_engine(url)
        return engine

    def read_mySQL(sql_script,conn_id):
        con = MySql.read_conn(conn_id)
        data = {}
        data = pd.read_sql_query(
            sql=sql_script,
            con=con
        )
        con.close()
        return data

    def read_rawMySQL(sql_script,conn_id):
        con = MySql.read_conn(conn_id)
        cursor = con.cursor()
        cursor.execute(sql_script)
        con.close()
        return cursor

    def dml_DWH(method, table_name, dml_sql):
        engine = MySql.write_conn()
        with engine.connect() as con:
            con.execute(dml_sql.format(method=method, table_name=table_name))
            print('{method} table "{table}" successfully'.format(method=method, table=table_name))
            con.close()

    def write_DWH(df,method,alt_script=None):
        engine = MySql.write_conn()
        table_name=df.name
        if method == 'replace':
            df.to_sql(name=table_name, con=engine, if_exists=method, index=False)
            if alt_script is not None:
                with engine.connect() as con:
                    con.execute(alt_script.format(table_name=table_name)
                    )
            else:
                pass
            print("replace data in DWH successfully.")
        elif method == 'append':
            try:
                df.to_sql(name=table_name, con=engine, if_exists=method, index=False)
                print('Append data to DWH successfully')
            except  Exception as e:
                Air.writeLog(e)
                raise Exception(e)

class Ga(Air):
    def __init__(self):
        conn_id='GA_conn'
        super().__init__(conn_id)

    def initialize_analyticsreporting():
        credentials = a_path.from_service_account_file(Ga().KEY_FILE, scopes=Ga().SCOPES)
  # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)
        return analytics

    def get_report(analytics, date, DIMENSIONS, METRICS):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': Ga().PROJECT_ID,
    #             'pageToken': '10000',
                'pageSize': 100000,
                'dateRanges': [{'startDate': date, 'endDate': date}],
                'metrics': [{'expression':i} for i in METRICS],
                'dimensions': [{'name':j} for j in DIMENSIONS],
            'samplingLevel': 'LARGE',
                }]
            }
        ).execute()

    def convert_to_dataframe(response):
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = [i.get('name',{}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
            finalRows = []
            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                metrics = row.get('metrics', [])[0].get('values', {})
                rowObject = {}
                for header, dimension in zip(dimensionHeaders, dimensions):
                    rowObject[header] = dimension
                for metricHeader, metric in zip(metricHeaders, metrics):
                    rowObject[metricHeader] = metric
                finalRows.append(rowObject)

        dataFrameFormat = pd.DataFrame(finalRows)
        return dataFrameFormat

    def getDf(input_date, DIMENSIONS, METRICS):
        analytics = Ga.initialize_analyticsreporting()
        response = Ga.get_report(analytics, input_date, DIMENSIONS, METRICS)
        return Ga.convert_to_dataframe(response)   #df = pandas dataframe

#'dwh_mysql'

class BigQuery(Air):
    def __init__(self):
        conn_id='auphie-gcp'
        super().__init__(conn_id)

    def gcp_conf():
        credential = a_path.from_service_account_file(BigQuery().KEY_FILE)
        return {
            'project_id': BigQuery().PROJECT_ID,
            'credentials': credential
        }

    def read_gbq(sql,dialect='standard'):
        try:
            data=pd.read_gbq(query=sql,dialect=dialect,**BigQuery.gcp_conf())
            return data
        except pandas_gbq.gbq.GenericGBQException:
            return pd.DataFrame()

    def drop_gbq(table_name, dialect='standard'):
        sql="drop table golden.{}".format(table_name)
        pd.read_gbq(query=sql,dialect=dialect,**BigQuery.gcp_conf())
        return False

    def write_gbq(df,method):
        df.to_gbq(
            chunksize=1024,
            destination_table='golden.{}'.format(df.name),
            if_exists=method,
            **BigQuery.gcp_conf()
        )


class Firebase(Air):
    def __init__(self):
        conn_id='auphie-firebase'
        super().__init__(conn_id)

    def read_gbq(sql,dialect='standard'):
        cred = a_path.from_service_account_file(Firebase().KEY_FILE)
        p_id = Firebase().PROJECT_ID
        try:
            data=pd.read_gbq(query=sql,dialect=dialect, project_id=p_id, credentials=cred)
            return data
        except pandas_gbq.gbq.GenericGBQException:
            return pd.DataFrame()


class Gsheet(Air):
    def __init__(self):
        conn_id='auphie-Gsheet'
        super().__init__(conn_id)

    def get2df(sheet_id, ranges):
        scopes = Gsheet().SCOPES
        secret_file = Gsheet().KEY_FILE
        credentials = a_path.from_service_account_file(secret_file, scopes=scopes)
        service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id,range=ranges).execute()
        data = result.get('values', [])
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
