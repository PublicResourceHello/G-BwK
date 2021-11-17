CREATE TABLE IF NOT EXISTS ae_intelligence_alg.ae_touch_push_cb_param_v2
(
    template_id STRING COMMENT ''
    ,param_a STRING COMMENT ''
    ,param_ani STRING COMMENT ''
    ,param_b STRING COMMENT ''
    ,param_theta STRING COMMENT ''
)
PARTITIONED by 
(
    ts STRING COMMENT ''
)
LIFECYCLE 14
;

SET odps.sql.mapper.memory=4096;

SET odps.sql.reducer.memory=4096;

SET odps.sql.joiner.memory=4096;

DROP TABLE IF EXISTS ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step0 ;

CREATE TABLE IF NOT EXISTS ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step0 LIFECYCLE 1 AS
SELECT  a.template_id
        ,coalesce(b.param_a , IDENTITY_MATRIX(3)) AS param_a
        ,coalesce(b.param_ani , IDENTITY_MATRIX(3)) AS param_ani
        ,coalesce(b.param_b , param_theta_all) AS param_b
        ,coalesce(b.param_theta , param_theta_all) AS param_theta
FROM    (
            SELECT  *
            FROM    (
                        SELECT  coalesce(
                                    concat(main_template_id , ':' , sub_template_id)
                                    ,'ALL'
                                ) AS template_id
                        FROM    ae_intelligence_alg.ae_push_job_total_budget_double
                        LATERAL VIEW EXPLODE (split(sub_template_infor , ',')) tmp AS sub_template_id
                        LATERAL VIEW EXPLODE (split(main_template_infor , ',')) tmp AS main_template_id
                        WHERE   ts = '${datetime}'
                        AND     id = 0
                        GROUP BY concat(
                                     main_template_id
                                     ,':'
                                     ,sub_template_id
                                 ) GROUPING sets( (), (concat(main_template_id , ':' , sub_template_id)) )
                    ) t
            LATERAL VIEW EXPLODE (split(( SELECT param_theta FROM ae_intelligence_alg.ae_touch_push_cb_param_v2 WHERE ts = MAX_PT("ae_intelligence_alg.ae_touch_push_cb_param_v2") AND template_id = 'ALL' ), '_')) tmp AS param_theta_all
        ) a
LEFT OUTER join (
                    SELECT  *
                    FROM    ae_intelligence_alg.ae_touch_push_cb_param_v2
                    WHERE   ts = MAX_PT("ae_intelligence_alg.ae_touch_push_cb_param_v2")
                    AND     param_a <> '1.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0'
                ) b
ON      a.template_id = b.template_id
;

DROP TABLE IF EXISTS ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step1 ;

CREATE TABLE IF NOT EXISTS ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step1 LIFECYCLE 1 AS
SELECT  template_id
        ,MATRIX_GROUPSUM(1, dot) AS param_a
        ,MATRIX_GROUPSUM(1, b_x) AS param_b
FROM    (
            SELECT  template_id
                    ,MATRIX_UPDATE_v2(concat('1,' ,feature ,'')) AS dot
                    ,case    WHEN is_clicked =1 THEN concat('[1,' ,feature ,']') 
                             ELSE '[0,0,0]' 
                     END AS b_x
            FROM    ae_intelligence_alg.ae_touch_push_bwk_feature_v2_d
            WHERE   ts = MAX_PT("ae_intelligence_alg.ae_touch_push_bwk_feature_v2_d")
            UNION ALL
            SELECT  template_id
                    ,concat('[' ,param_a ,']')
                    ,concat('[' ,param_b ,']')
            FROM    ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step0
            WHERE   template_id <> 'ALL'
        ) b
GROUP BY template_id
UNION ALL
SELECT  'ALL' AS template_id
        ,MATRIX_GROUPSUM(1, dot) AS param_a
        ,MATRIX_GROUPSUM(1, b_x) AS param_b
FROM    (
            SELECT  template_id
                    ,MATRIX_UPDATE_v2(concat('1,' ,feature ,'')) AS dot
                    ,case    WHEN is_clicked =1 THEN concat('[1,' ,feature ,']') 
                             ELSE '[0,0,0]' 
                     END AS b_x
            FROM    ae_intelligence_alg.ae_touch_push_bwk_feature_v2_d
            WHERE   ts = MAX_PT("ae_intelligence_alg.ae_touch_push_bwk_feature_v2_d")
            UNION ALL
            SELECT  template_id
                    ,concat('[' ,param_a ,']')
                    ,concat('[' ,param_b ,']')
            FROM    ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step0
            WHERE   template_id = 'ALL'
        ) a
GROUP BY 'ALL'
;

INSERT OVERWRITE TABLE ae_intelligence_alg.ae_touch_push_cb_param_v2 PARTITION(ts = '${datetime}')
SELECT  template_id
        ,param_a
        ,param_ani
        ,param_b
        ,matrix_dot(param_ani , param_b) AS param_theta
FROM    (
            SELECT  a.template_id
                    ,coalesce(b.param_a , a.param_a) AS param_a
                    ,matrix_ni(coalesce(b.param_a , a.param_a)) AS param_ani
                    ,coalesce(b.param_b , a.param_b) AS param_b
            FROM    ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step0 a
            LEFT OUTER JOIN ae_intelligence_alg.tmp_ae_touch_push_cb_param_v2_${datetime}_step1 b
            ON      a.template_id = b.template_id
        ) t
;
