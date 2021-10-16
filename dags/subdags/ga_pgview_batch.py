import pandas as pd
from bridge import Ga
from bridge import BigQuery as bq
from datetime import timedelta, date

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date ).days + 1)):
        yield start_date + timedelta(n)


if __name__ == '__main__':
    DIMENSIONS = ['ga:date','ga:pagePath']
    METRICS = ['ga:pageviews','ga:uniquePageviews']
    start_date = date(2021,5,1)
    end_date = date(2021,5,1)

    for insert_date in daterange(start_date, end_date):
        insert_date=insert_date.strftime("%Y-%m-%d")
        df = Ga.getDf(insert_date, DIMENSIONS, METRICS)
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
            hist_df = pd.DataFrame({'Date':['2100-12-31']})
            hist_df['Date']=pd.to_datetime(hist_df['Date'], format = '%Y-%m-%d')
            hist_date = hist_df.Date.dt.date
            print('hist_df is empty')
        else:
            dates = hist_df.Date.dt.date.unique()
            hist_date = [str(d) for d in dates]
        if set([insert_date]).issubset(hist_date):
            print("Insert BigQuery error, duplicate data by same date insert!")
        else: bq.write_gbq(df,'append')
