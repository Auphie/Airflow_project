import requests
import sys, io
import pandas as pd
from datetime import datetime, timedelta, date
from bridge import MySql

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date ).days + 1)):
        yield start_date + timedelta(n)

url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
# url = 'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_daily_reports/'

def single_insert(single_date):
    input_date = single_date.strftime("%m-%d-%Y")
    r = requests.get(url + '{date}.csv'.format(date=input_date))
    if r.status_code == 200:
        to_df(r, input_date)
    else:
        sys.exit()

def batch_insert(start_date, end_date, hist_date):
    for single_date in daterange(start_date, end_date):
        if single_date in hist_date:
            print("Insert DWH error, duplicate data by same date insert!")
            pass
        else:
            single_insert(single_date)

def to_df(r, input_date):
    df = pd.read_csv(io.BytesIO(r.content), sep=',', error_bad_lines=False, encoding='utf8')
#    df = df.drop(columns=['Last Update'])
#    df = df.rename(columns={'Country/Region':'country','Province/State':'city'})

#    df = df.drop(columns=['FIPS','Admin2','Combined_Key','Lat','Long_','Last_Update','Incidence_Rate','Case-Fatality_Ratio'])
    df = df.rename(columns={'Country_Region':'country','Province_State':'city'})
    df1 = df[['country','city','Confirmed','Deaths','Recovered','Active']]
    df1['q_date'] = datetime.strptime(input_date,'%m-%d-%Y')
#    df['Last_Update'] = pd.to_datetime(df['Last_Update'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df1.name = 'covid_19'
    MySql.write_DWH(df1,'append')

sql_dw="""select distinct date(q_date) 'q_date' from covid_19"""


if __name__ == '__main__':
    start_date = date(2020,5,2)
    end_date = date(2020,6,27)
    conn_dw='dwh_mysql'

    hist_df = MySql.read_mySQL(sql_dw,conn_dw)
    hist_date = hist_df.q_date.unique()

#    batch_insert(start_date, end_date, hist_date)

    yesterday = date.today() - timedelta(days=1)
    print(yesterday)

    if yesterday in hist_date:
        print("Insert DWH error, duplicate data by same date insert!")
        pass
    else:
        single_insert(yesterday)
