import pandas as pd
import numpy as np
from bridge import MySql

sql1="""SELECT concat(right(DATE_FORMAT(date_add(now(),interval +8 hour),'%Y%m%d'),6),lpad(ld.field_xx,5,'0')) 'pkey',
		timestamp(date_add(now(),interval +8 hour)) 'rec_date',
		ld.field_xx 'country_id', ld.field_xx 'city_id',
		ld.field_xx 'area_id', ld.field_xx 'bnb_id', par.`name` 'partner', ay.field_xx,
		case ld.field_xx when 1 then 'Y' else 'N' end 'legal_tick',
		case when ib.field_xx = 3 and ib.field_xx <> 'off' then 'Y' else 'N' end 'is_IB',
		tag.field_xx, l.field_xx 'host_email',
		case ld.field_xx when 1 then 'Y' else 'N' end   'can_book_today'
from table_1 		 as ld
join table_2 			 as l	    on ld.field_xx = l.field_xx
join table_3 			 as co	    on ld.field_xx = co.field_xx
join table_4		 as p1 	    on ld.field_xx = p1.field_xx
join table_5 		 as p4      on p1.field_xx = p4.field_xx
join table_6 		 as ll      on ld.field_xx = ll.field_xx
join table_7 			 as ci      on ld.field_xx = ci.field_xx
join table_8 			 as a       on ld.field_xx = a.field_xx
left join table_9 as ay      on ld.field_xx = ay.field_xx
left join table_10 	 as par     on ld.field_xx = par.field_xx
left join table_11	 as ib      on ld.field_xx = ib.field_xx
	 and ib.field_32 = 3 and ib.field_xx <> 'off'
left join table_12 as ldbw on ldbw.field_xx = ld.field_xx
left join (select z.field_xx, group_concat(z.field_xx SEPARATOR ', ') field_xx
	from (	select t.field_xx, l.field_xx, t.field_xx, t.field_xx, t.field_xx,
		  t.field_xx, m.field_xx
		from table_13 as t
		join table_14 as m
		  on t.field_xx = m.field_xx
		join table_15 as l
		  on t.field_xx = l.field_xx
		where t.field_xx = 1
		  and ifnull(t.field_xx,'2099-12-31 00:00:00') > now()
  		  and ifnull(t.field_xx,'2000-12-31 00:00:00') < now()
		  and l.field_xx = 9
		  and l.field_xx = 1
		) as z
	group by z.field_xx
	) as tag  on ld.field_xx = tag.field_xx
where exists (select field_xx from table_16 where p1.field_xx = field_xx)
  and p4.field_xx > 0
	  and ld.field_xx not in (select field_xx from table_17 where field_xx='l_id')
	  and ld.field_xx = ''
	  and ld.field_xx = 'on'
	  and (ifnull(ldbw.field_xx,'') <> 'Y' or ld.field_xx = 1)
	  and (p1.field_xx * (p1.field_xx + p1.field_xx) >= 1)
	  and (p1.field_xx != 'N' or p1.field_xx is null)
	  and p1.field_xx = 'Y'
group by ld.field_xx"""

def add_AY_info(df):
    df['***'] = df.sort_values(['***'], ascending=[0,1]) \
             .groupby(['***']).cumcount() + 1
    df['***'] = np.ceil(df.***.div(20)).astype(int)
    df.sort_values(by=['***'], ascending=1)
    return df


if __name__ == '__main__':
    conn_1='auphie-mysql'
    df1 = MySql.read_mySQL(sql1,conn_1)
    df1.drop_duplicates(subset=None,keep='first',inplace=True)
    df1=add_AY_info(df1)
    df1['field_xx']=np.where(df1['host_email'].str.match('(xx|yy|zz|ww)\d+@auphie.com'),'Y','N')
    df1.drop('field_xx', axis=1, inplace=True)
    df1.name='table_18'
    MySql.write_DWH(df1,'append')
