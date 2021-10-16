import pandas as pd
from bridge import Firebase as fb
from bridge import MySql
from datetime import timedelta, date

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date ).days + 1)):
        yield start_date + timedelta(n)

raw_bq="""SELECT user_pseudo_id, event_date, event_name,
a.value.string_value as ***,
b.value.string_value as ***,
c.value.string_value as ***,
d.value.string_value as ***,
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
where a.key = '***'
  and b.key = '***'
  and c.key = '***'
  and d.key = '***'
  and a.value.string_value <> 'N/A'"""

hist_date="""select distinct event_date from ***"""

if __name__ == '__main__':
    start_date = date(2021,6,2)
    end_date = date(2021,6,3)
    conn_dw='dwh_mysql'

    hist_df = MySql.read_mySQL(hist_date,conn_dw)
    hist_date = hist_df.event_date.unique()

    for query_date in daterange(start_date, end_date):
        if query_date in set(hist_date):
            print("Insert DWH error, duplicate data by same date insert!")
        else:
            query_date=query_date.strftime("%Y%m%d")
            df = fb.read_gbq(raw_bq.format(query_date=query_date))
            df.dropna(subset=['event_date','***_date','***_date'], inplace=True)
            df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d', errors='coerce')
            df['***_date']= pd.to_datetime(df['***_date'], errors='coerce')
            df['***_date']= pd.to_datetime(df['***_date'], errors='coerce')
            df.name='***'
            MySql.write_DWH(df,'append')
