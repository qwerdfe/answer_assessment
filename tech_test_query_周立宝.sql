set "dt.report" = '2020-08-17';

select to_date(current_setting('dt.report'), 'yyyy-MM-dd') as dt_report,
       login_hash,
       server_hash,
       symbol,
       currency,
       sum(case when flag_prev_7d = 1 then volume
              else 0 
            end
         ) ::double precision as sum_volume_prev_7d,
       sum(case when close_time < report_end_date  
                then volume
              else 0 
           end
          ) ::double precision as sum_volume_prev_all,
       dense_rank() over(order by max(volume_for_sort) desc ) as rank_volume_symbol_prev_7d,
       dense_rank() over(order by max(trade_count_for_sort) desc ) as rank_count_prev_7d,
       sum(case when close_time >= to_date('2020-08-01', 'yyyy-MM-dd') 
                and close_time < report_end_date 
                and close_time < to_date('2020-09-01', 'yyyy-MM-dd')
                then volume
             else 0 
          end
        ) ::double precision as sum_volume_2020_08,
       min(case when close_time < report_end_date  
                then close_time
             else null 
         end) as date_first_trade,
      row_number() over(order by current_setting('dt.report'), login_hash,server_hash,symbol) as row_number
from
(   
  select *,
         max(case when flag_prev_7d = 1 then volume
               else 0 
         end) over(partition by login_hash,symbol) as volume_for_sort,
         sum(case when flag_prev_7d = 1 then 1
                 else 0 
         end) over(partition by login_hash) as trade_count_for_sort
 from
  ( 
    select t.login_hash,
           t.server_hash,
           t.symbol,
           u.currency,
           t.volume,
           t.close_time,
           case when close_time >=      
                      to_date(current_setting('dt.report'), 'yyyy-MM-dd') - interval '7 day' 
                    and close_time < 
                     to_date(current_setting('dt.report'), 'yyyy-MM-dd') + interval '1 day' 
            then 1 else 0 end as flag_prev_7d,
           to_date(current_setting('dt.report'), 'yyyy-MM-dd') + interval '1 day' as report_end_date
   from trades as t
   join
     (select distinct 
                trim(login_hash) as login_hash,
                trim(server_hash) as server_hash,
                trim(currency) as currency
            from users 
         where enable=1
     ) as u
    on trim(t.login_hash)=u.login_hash
    and trim(t.server_hash)=u.server_hash
  ) as m


) as a
group by login_hash,server_hash,symbol,currency
order by row_number desc;