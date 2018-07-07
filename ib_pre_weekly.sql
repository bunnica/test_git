select  week_start,
        week_arrange,
        count(ib.sessionid) as connect_num,
        count(pre.sessionid) as connect_num_pre,
        round(count(pre.sessionid)/count(ib.sessionid),2) as pre_ratio,
        count(distinct pre.ani) as connect__pre_user,
        count(distinct od.ani) as order_user,
        round(count(distinct od.ani)/count(distinct pre.ani),2) as zh_ratio
from 
(select sessionid,
        ani,
        substring(establishedtimestamp,1,10) as stat_date
from  zw_ods.ods_zw_ib_zwlc_session_info
            where dt='$PARTITION_DATE'
                  and direction=1
                  and establishedtimestamp is not null
)ib
left join
(select sessionid,
        ani,
        agentid,
        realname,
        substring(establishedtimestamp,1,10) as stat_date,
        releasedtimestamp,
        amount,
        off_amount
from
    (
    select sessionid,
            ani,
            case when agentid='100016' then '班秀燕'
                 when agentid='100391' then '孙丽英'
                 when agentid='100068' then '肖旭'
                 when agentid='100466' then '周雪娜'
                 when agentid='100380' then '张晶'
                 when agentid='100531' then '张亚同'
                 when agentid='100011' then '吕晓阳'
                 when agentid='100101' then '孟玲'
                 when agentid='100058' then '杨双兰'
                 when agentid='100504' then '郭萌'
                 when agentid='100067' then '李东东'
                 when agentid='100063' then '李蓓'
                 when agentid='100539' then '张学佳'
                 when agentid='100540' then '张轩' 
                 else NULL
                 end  as realname,
            agentid,
            establishedtimestamp,
            releasedtimestamp
            from  zw_ods.ods_zw_ib_zwlc_session_info
            where dt='$PARTITION_DATE'
                  and direction=1
                  and establishedtimestamp is not null
    )c
    join
       (
        select  user_id,
                mobile_phone
        from zw_dm.dm_user_basic_info
        where dt='$PARTITION_DATE' and bp_broker_id=0 
        )a
    on a.mobile_phone=c.ani
   join
  (
       select user_id,
              acc_order_num,
              first_order_amount/100 as amount,
              first_order_date,
              first_order_id
       from zw_dm.dm_user_order_info
       where dt='$PARTITION_DATE'    
      )f
     on f.user_id=a.user_id  
    join
        (
        select user_id,order_id,off_amount/100 as off_amount,updated_at
        from zw_dm.dm_create_order_info
        where dt='$PARTITION_DATE' and status = 200 and order_type not in (2,5,8)
        )t1
     on t1.order_id=f.first_order_id and f.user_id=t1.user_id
 where substring(f.first_order_date,1,10)=substring(c.releasedtimestamp,1,10) and f.first_order_date>=c.releasedtimestamp
 )od
on ib.stat_date=od.stat_date and ib.sessionid = od.sessionid

left join 

(SELECT tt31.sessionid,
       tt31.ani,
       tt33.first_order_date,
       substring(tt31.establishedtimestamp,1,10) AS stat_date,
       tt32.user_id
FROM
     (SELECT sessionid,
             establishedtimestamp,
             releasedtimestamp,
             ani
      FROM zw_ods.ods_zw_ib_zwlc_session_info
      WHERE dt='$PARTITION_DATE'
        AND direction=1
        AND establishedtimestamp IS NOT NULL )tt31
LEFT  JOIN
  ( SELECT user_id,
           mobile_phone
   FROM zw_dm.dm_user_basic_info
   WHERE dt='$PARTITION_DATE')tt32 
ON tt32.mobile_phone=tt31.ani
LEFT JOIN
  ( SELECT user_id,
           acc_order_num,
           first_order_amount/100 AS amount,
           first_order_date,
           first_order_id
   FROM zw_dm.dm_user_order_info
   WHERE dt='$PARTITION_DATE'
     )tt33 ON tt32.user_id=tt33.user_id
WHERE (first_order_date >=tt31.establishedtimestamp or first_order_date is null) and length(ani)=11 and tt32.user_id is not null 
)pre
on pre.stat_date=ib.stat_date and pre.sessionid = ib.sessionid
join 

(select week_start,
        week_end,
        week_arrange,
        str_date
from zw_dim.dim_weeknum 
)tt4

on ib.stat_date=tt4.str_date
where tt4.week_start = '$STAT_DATE'
group by week_start,
        week_arrange