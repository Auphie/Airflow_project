from bridge import Ga
from bridge import MySql
import pandas as pd
from datetime import timedelta, date

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date ).days + 1)):
        yield start_date + timedelta(n)


if __name__ == '__main__':
    DIMENSIONS = ['ga:date','ga:landingPagePath','ga:secondPagePath']
    METRICS = ['ga:sessions','ga:transactions']
    start_date = date(2019,11,1)
    end_date = date(2019,11,10)

    for single_date in daterange(start_date, end_date):
        input_date=single_date.strftime("%Y-%m-%d")
        df = Ga.getDf(input_date, DIMENSIONS, METRICS)
        df.rename(columns={'ga:date':'Date',
                        'ga:landingPagePath':'landPage',
                        'ga:secondPagePath':'secondPage',
                        'ga:sessions':'sessions',
                        'ga:transactions':'transactions'
                        },inplace=True)
        df2 = df[df['landPage'].str.contains(r'.+/xxx/[A-Za-z0-9]+', na=False, regex=True)]
        df2 = df2.copy()
        df2['Date'] = pd.to_datetime(df2.Date, format='%Y%m%d', errors='coerce').dt.date
        df2['***'] = df2['landPage'].str.extract(r'/([a-z]{2}-[a-z]{2})/')
        df2['***'] = df2['landPage'].str.extract(r'.+/xxx/([A-Za-z0-9_\-]+)/')
        df2['***'] = df2['landPage'].str.extract(r'/list/([a-z]{2})/')
        df2['***'] = df2['landPage'].str.extract(r'/list/[a-z]{2}/([A-Za-z\-]+)/')
        df2['***'] = df2['landPage'].str.split('/').str[7].str.extract(r'aff_id=(\d+)')
        df2['***'] = df2['secondPage'].str.extract(r'.+/view/.+/(\d{2,7})/')
        df2['sessions'] = pd.to_numeric(df2.sessions.str.replace(',', ''),errors ='coerce').astype(int)
        df2.drop(df2[df2['***']==''].index, inplace=True)
        df2.name = '***'
        insert_date = list(pd.to_datetime(df2.Date).apply(lambda x: x.date()))

        conn_dw='dwh_mysql'
        sql_dw="""select distinct Date from ***"""
        hist_df = MySql.read_mySQL(sql_dw,conn_dw)
        hist_date = hist_df.Date.unique()
        if set(insert_date).issubset(hist_date):
            print("Insert DWH error, duplicate data by same date insert!")
        else:
            MySql.write_DWH(df2,'append')