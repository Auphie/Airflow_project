import pandas as pd
from bridge import Firebase as fb
from bridge import MySql
from bridge import BigQuery as bq
from datetime import timedelta, date

table_name='GA4_***'

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date ).days + 1)):
        yield start_date + timedelta(n)

raw_bq="""SELECT distinct user_pseudo_id, event_date, event_name,
a.value.string_value as ***_date,
b.value.string_value as ***_date,
c.value.string_value as ***,
d.value.string_value as ***,
e.value.int_value as ga_session_id,
geo.country as user_country,
geo.region as user_region,
geo.city as user_city,
traffic_source.source,
platform,
device.category as device
FROM `***.analytics_***.events_{query_date}`
, UNNEST(event_params) as a
, UNNEST(event_params) as b
, UNNEST(event_params) as c
, UNNEST(event_params) as d
, UNNEST(event_params) as e
where a.key = '***_date'
  and b.key = '***_date'
  and c.key = '***'
  and d.key = '***'
  and e.key = 'ga_session_id'
  and a.value.string_value <> 'N/A'"""

sql_bq="""select date(z.event_date) as `Date` from (
        SELECT `event_date` FROM ***.%s group by event_date order by 1 desc) as z"""%table_name

if __name__ == '__main__':
    start_date = date(2021,1,2)
    end_date = date(2021,1,31)
#    day2before = date.today() - timedelta(days=2)
    hist_df = bq.read_gbq(sql_bq)
    if hist_df.empty:
        dates = pd.DataFrame({'Date':['2100-12-31']})
        print('Table has not created.')
    else:dates = hist_df.Date.dt.date.unique()

    for query_date in daterange(start_date, end_date):
#    for query_date in daterange(day2before, day2before):
        if set([query_date]).issubset(dates):
            print("Insert BigQuery error, duplicate data by same date insert!")
        else:
            query_date = query_date.strftime("%Y%m%d")
            df = fb.read_gbq(raw_bq.format(query_date=query_date))
            df.dropna(subset = ['***'], inplace=True)
            df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d', errors='coerce')
            df['event_month'] = df['event_date'].dt.month
            df['***_date'] = pd.to_datetime(df['***_date'], errors='coerce')
            df['***_date'] = pd.to_datetime(df['***_date'], errors='coerce')
            df['ga_session_id'] = df['ga_session_id'].astype(str)
            df.name = table_name
            bq.write_gbq(df,'append')

            conn_dw='dwh_mysql'
            MySql.write_DWH(df,'append')
