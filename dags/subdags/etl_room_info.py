import pandas as pd
import numpy as np
from bridge import MySql

## Note that off-line room_id could not be found in online bnb!
## PS: bnb has both online & offline status causes table ambiguous easily.
## Plz be careful when you treat room-related queries or research!

selected_column="""a lot of repetitive fields!!!'"""

joint_table="""around 20 tables"""

sql1="""select 'on' as 'online',, {selected_column}
    from different tables and joint relations
    {joint_table}
where online criteria
UNION
select 'off' as 'online', {selected_column}
    from different tables and joint relations
    {joint_table}
where offline criteria""".format(selected_column=selected_column, joint_table=joint_table)

sql2="""select *** 'room_id', case when *** end 'de_rooms'
from {table} as s
join ***
where exists (select *** from *** where *** = {country_id})
  and date(***) between date(now())+1 and date(date_add(now(), interval {schedule_weeks} week))
group by ***"""

def deduct_rooms(sql2,conn_id):
    results = []
    param_spot = [[1,'***', 12],
            [2,'***',12],
            [5,'***',12],
            [6,'***',12],
            [8,'***',12],
            [10,'***',12],
            [13,'***',12]]

    for i in range(0,len(param_spot)):
        country_id = param_spot[i][0]
        table = param_spot[i][1]
        schedule_weeks = param_spot[i][2]
        sql=sql2.format(table=table,schedule_weeks=schedule_weeks,country_id=country_id)
        results.append(MySql.read_mySQL(sql,conn_id))

    data = pd.concat(results)
    return data


if __name__ == '__main__':
    conn_id='auphie-mysql'
    df1 = MySql.read_mySQL(sql1,conn_id)
    df2 = deduct_rooms(sql2,conn_id)
    df = pd.merge(df1, df2, how='left', on='***')
    df['***'] = pd.to_datetime(df['***']).dt.time
    df['de_rooms'].fillna(value=0, inplace=True)
    df['adj_rooms']=np.where(df['partner'].str.match('(***)')  \
                             ,df['tot_rooms']-df['de_rooms'],df['tot_rooms'])
    df['cap_rooms']=np.where(df['tot_rooms']-df['de_rooms']>0,df['tot_rooms']-df['de_rooms'],0)
#    df['r_online']=np.where(df['tot_rooms']-df['de_rooms']<=0,'off',df['online'])
    df['***']=np.where(df['email'].str.match('(***|***|***)\d+@auphie.com')
                        |df['email'].str.match('(^05|^10|^15)@auphie.com'),'Y','N')
    df.name='***'
    MySql.write_DWH(df,'replace',alt_script)

