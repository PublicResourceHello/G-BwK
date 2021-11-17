CREATE TABLE IF NOT EXISTS ae_intelligence_alg.ae_touch_push_bwk_dual_min_v2
(
    job_id BIGINT COMMENT ''
    ,need_sent_cnt BIGINT COMMENT ''
    ,theta DOUBLE COMMENT ''
    ,epcilon DOUBLE COMMENT ''
    ,dual DOUBLE COMMENT ''
    ,iscons BIGINT COMMENT ''
)
PARTITIONED by 
(
    ts STRING COMMENT ''
)
LIFECYCLE 7
;

DROP TABLE IF EXISTS ae_intelligence_alg.ae_touch_push_bwk_dual_min_v2_${datetime}_step0 ;

CREATE TABLE IF NOT EXISTS ae_touch_push_bwk_dual_min_v2_${datetime}_step0 LIFECYCLE 1 AS
SELECT  count(DISTINCT job_id) AS cnt
FROM    ae_intelligence_alg.ae_touch_push_allocation_d
WHERE   ds = to_char(dateadd(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , - 2,'dd') ,'yyyymmdd')
;

INSERT OVERWRITE TABLE ae_intelligence_alg.ae_touch_push_bwk_dual_min_v2 PARTITION(ts = '${datetime}')
SELECT  t.job_id
        ,t.need_sent_cnt
        ,case    WHEN coalesce(consumed_uv_num , 0) >= ceil(num/cast(proportion_cnt AS DOUBLE)) THEN 0
                 WHEN datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'hh') = 0 AND datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'mi') = 0 THEN 1.0 / (1+t.epcilon) 
                 ELSE coalesce(c.theta , 1.0)/(1+t.epcilon) 
         END AS theta
        ,t.epcilon
        ,case    WHEN coalesce(consumed_uv_num , 0) >= ceil(num/cast(proportion_cnt AS DOUBLE)) THEN 1
                 WHEN datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'hh') = 0 AND datepart(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , 'mi') = 0 THEN 1/(1 - 1.0 / (1+t.epcilon)) 
                 ELSE 1/(1 - coalesce(c.theta , 1.0)/(1+ t.epcilon )) 
         END AS daul
        ,t.iscons
FROM    (
            SELECT  a.job_id
                    ,consumed_uv_num
                    ,num
                    ,proportion_cnt
                    ,a.target_num_now - coalesce(b.consumed_uv_num , 0) AS need_sent_cnt
                    ,case    WHEN a.target_num_now - coalesce(b.consumed_uv_num , 0) <=0 THEN 0 
                             ELSE coalesce(sqrt(ln(param_cnt)/((a.target_num_now - coalesce(b.consumed_uv_num , 0)))) , 0) 
                     END AS epcilon
                    ,dual_tmp
                    ,target_num_now
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
                        FROM    ae_intelligence_alg.ae_touch_push_allocation_d
                        LATERAL VIEW EXPLODE (SPLIT( (SELECT config FROM ae_intelligence_alg.ae_touch_push_allocation_config ) ,',')) tmp AS param
                        LATERAL VIEW EXPLODE (SPLIT( (SELECT cnt FROM ae_touch_push_bwk_dual_min_v2_${datetime}_step0 ) ,',')) tmp AS param_cnt
                        WHERE   ds = to_char(dateadd(bi_udf:bi_changetimezone(TO_DATE('${datetime}' ,'yyyymmddhhmiss') , 'GMT+8' , 'PST') , - 2,'dd') ,'yyyymmdd')
                        AND     hh = 23
                        AND     mm = 45
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
        ) t
LEFT OUTER join (
                    SELECT  *
                    FROM    ae_intelligence_alg.ae_touch_push_bwk_dual_min_v2
                    WHERE   ts = MAX_PT("ae_intelligence_alg.ae_touch_push_bwk_dual_min_v2")
                ) c
ON      t.job_id = c.job_id
AND     t.iscons = c.iscons
;
