import pandas as pd
from bridge import Ga
from bridge import BigQuery as bq
from datetime import date, timedelta


if __name__ == '__main__':
    DIMENSIONS = ['ga:date','ga:pagePath']
    METRICS = ['ga:pageviews','ga:uniquePageviews']
    insert_date = date.today() - timedelta(days=1)
    df = Ga.getDf(str(insert_date), DIMENSIONS, METRICS)
    df.rename(columns={'ga:date':'Date',
                        'ga:pagePath':'Page',
                        'ga:pageviews':'Pageviews',
                        'ga:uniquePageviews':'uniquePageviews'
                        },inplace=True)

    df['Pageviews'] = pd.to_numeric(df.Pageviews.str.replace(',', ''),errors ='coerce')
    df['uniquePageviews'] = pd.to_numeric(df.uniquePageviews.str.replace(',', ''),errors ='coerce')
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d', errors='coerce')
    df.name='***'
    sql_bq="""select date(z.Date) as `Date` from (
              SELECT `Date` FROM ***.%s group by `Date` order by 1 desc) as z"""%df.name
    hist_df = bq.read_gbq(sql_bq)
    if hist_df.empty:
        hist_df = pd.DataFrame({'Date':['2000-01-01']})
        hist_df['Date']=pd.to_datetime(hist_df['Date'], format = '%Y-%m-%d')
        hist_date = hist_df.Date.dt.date
        print('hist_df is empty')
    else: hist_date = hist_df.Date.dt.date.unique()
    if set([insert_date]).issubset(hist_date):
        print("Insert BigQuery error, duplicate data by same date insert!")
    else: bq.write_gbq(df,'append')