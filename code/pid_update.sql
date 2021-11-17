CREATE TABLE IF NOT EXISTS ae_intelligence_alg.ae_touch_push_allocation_job_min_tmp
(
    job_id BIGINT COMMENT ''
    ,dual DOUBLE COMMENT ''
    ,p_t BIGINT COMMENT ''
    ,d_t BIGINT COMMENT ''
    ,isCons BIGINT COMMENT ''
)
PARTITIONED by 
(
    ts STRING COMMENT ''
)
LIFECYCLE 7
;

INSERT OVERWRITE TABLE ae_intelligence_alg.ae_touch_push_allocation_job_min_tmp PARTITION(ts = '${datetime}')
SELECT  a.job_id
        ,case    WHEN coalesce(b.consumed_uv_num , 0) >= ceil(a.num/cast(proportion_cnt AS DOUBLE)) THEN 0
                 WHEN datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'hh') = 0 AND datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'mi') = 0 THEN dual_tmp + (a.pid_p * (a.target_num_now - coalesce(b.consumed_uv_num , 0))) 
                 ELSE coalesce(c.dual ,a.dual_tmp) + (a.pid_p * (a.target_num_now - coalesce(b.consumed_uv_num , 0))) + (a.pid_d * (a.target_num_now - coalesce(b.consumed_uv_num , 0) - COALESCE(c.p_t , 0))) 
         END AS daul
        ,a.target_num_now - coalesce(b.consumed_uv_num , 0) AS p_t
        ,case    WHEN datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'hh') = 0 AND datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'mi') = 0 THEN 0 
                 ELSE a.target_num_now - coalesce(b.consumed_uv_num , 0) - COALESCE(c.p_t , 0) 
         END AS d_t
        ,a.iscons
FROM    (
            SELECT  *
                    ,split(param , ':')[0] AS iscons
                    ,split(param , ':')[1] AS pid_p
                    ,split(param , ':')[2] AS pid_d
                    ,split(param , ':')[3] AS dual_tmp
                    ,split(param , ':')[4] AS proportion_cnt
                    ,ceil(
                        target_num / cast(split(param , ':')[4] AS DOUBLE)
                    ) AS target_num_now
            FROM    ae_intelligence_alg.ae_touch_push_collabration_d_tmp
            LATERAL VIEW EXPLODE (SPLIT('10:0.000001:0.000001:0:100' ,',')) tmp AS param
            WHERE   ds = to_char(dateadd(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , - 2,'dd') ,'yyyymmdd')
            AND     hh = datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'hh')
            AND     mm = datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'mi')
        ) a
LEFT OUTER join (
                    SELECT  *
                    FROM    aeusertouch.ae_push_summary_by_iscons_mi
                    WHERE   ds = substr('${datetime}' , 1 ,8)
                    AND     hh = datepart(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'hh')
                    AND     mm = datepart(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'mi')
                ) b
ON      a.job_id = b.job_id
AND     a.iscons = b.iscons
LEFT OUTER join (
                    SELECT  *
                    FROM    ae_intelligence_alg.ae_touch_push_allocation_job_min_tmp
                    WHERE   ts = MAX_PT("ae_intelligence_alg.ae_touch_push_allocation_job_min_tmp")
                ) c
ON      a.job_id = c.job_id
AND     a.iscons = c.iscons
;
