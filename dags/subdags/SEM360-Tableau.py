#!/usr/bin/env python3
import pandas as pd

"""
tabdir=locatePath
tabserver=yourTableauServer
tabsite=TableauID
tabuser="TableauAccount"
tabpassword= "TableauPassword"
echo "====== `date` start tabcmd ====== " >>$tabdir/tabexport.log
## export a dashboard in Tableau Online to csv
/~tabcmd_path/tabcmd export -s "$tabserver" -u "$tabuser" -p "$tabpassword" --no-prompt  "SEM_Feeds_0/tab_name" --csv -f "$tabdir/SEM_FEEDS.csv" |tee -a $tabdir/tabexport.log
## import csv and then ETL to data warehouse or Google Sheets
./SEM360-Tableau.py
"""

data = "Google_SEM360.csv"
path = 'your path'

df = pd.read_csv(data,sep=',',encoding='utf8')

integrate = df.pivot_table(index=['country', 'city','PKey','spot_type','spot_id','spot_nmae','landing_page'],columns='Measure Names',values='Measure Values', aggfunc='first')
integrate.reset_index(inplace=True)
integrate = integrate[['country', 'city','PKey','spot_type','spot_id','spot_name','landing_page','min_NTD','bnbs','avl_room']]

integrate['min_NTD'] = pd.to_numeric(integrate.min_NTD.str.replace(',', ''),errors ='coerce')
integrate['bnbs'] = pd.to_numeric(integrate.bnbs.str.replace(',', ''),errors ='coerce')
integrate['avl_room'] = pd.to_numeric(integrate.avl_room.str.replace(',', ''),errors ='coerce')

#Spot_JP = integrate.drop(['spot_id'], axis=1)
Spot_JP = integrate
Spot_JP.to_csv(path+'jp/'+'Spot_JP.csv', sep=',',encoding='utf8', index=0)
print('Spot_JP.csv has created')

Spot_TW = integrate.loc[(integrate['country'] == '台灣') & (integrate['spot_type'] == 'spot')]
Spot_TW.to_csv(path+'tw/'+'Spot_TW.csv', sep=',',encoding='utf8', index=0)
print('Spot_TW.csv has created')

Subway_TW = integrate.loc[(integrate['country'] == '台灣') & (integrate['spot_type'] == 'subway')]
Subway_TW.to_csv(path+'tw/'+'Subway_TW.csv', sep=',',encoding='utf8', index=0)
print('Subway_TW.csv has created')

Dist_TW = integrate.loc[(integrate['country'] == '台灣') & (integrate['spot_type'] == 'area')]
District_TW = Dist_TW.drop(['spot_type', 'spot_id'], axis=1)
District_TW.rename(columns={'country':'國家','city':'城市','PKey':'Pkey','spot_nmae':'district'}, inplace=True)
District_TW.to_csv(path+'tw/'+'District_TW.csv', sep=',',encoding='utf8', index=0)
print('District_TW.csv has created')

# put your df to MySQL or Google Sheets, or a certain directory for Google SEM 360 service to retrieve your latest data