COPY (

-- Combine with BMI visit information
WITH semaglutide_study_period_w_bmi AS (
    SELECT 
        s.*,
        b.bmiw_visit_date,
        b.bmi_visit,
        b.weight_kg_visit,
        fl.first_visit_date,
        fl.last_visit_date,
        CASE 
            WHEN s.semaglutide_initiate_date_minus_24m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_minus_12m THEN '-2'
            WHEN s.semaglutide_initiate_date_minus_12m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date THEN '-1'
            WHEN s.semaglutide_initiate_date <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_12m THEN '0'
            WHEN s.semaglutide_initiate_date_plus_12m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_24m THEN '1'
            ELSE NULL 
        END AS period_12m
    FROM 
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_study_period.parquet') s 
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_initiate_patients_first_last_visit_date.parquet') fl      
        ON s.person_id = fl.person_id
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/bmiw_visit.parquet') b
        ON s.person_id = b.person_id
    WHERE 
        (s.semaglutide_initiate_date_plus_24m <= fl.last_visit_date)  -- Ensure in observation period
        AND (fl.first_visit_date <= s.semaglutide_initiate_date_minus_24m) -- Ensure in observation period
),

-- Select patients with BMI/weight data for all 5 periods
selected_patients_w_all_periods AS (
    SELECT 
        person_id,
        COUNT(DISTINCT period_12m) AS period_12m_count,
        MAX(semaglutide_initiate_date_plus_24m) AS study_period_end,
        MAX(last_visit_date) AS observe_period_end,
        MIN(semaglutide_initiate_date_minus_24m) AS study_period_start,
        MIN(first_visit_date) AS observe_period_start
    FROM 
        semaglutide_study_period_w_bmi
    GROUP BY 
        person_id
    HAVING 
        COUNT(DISTINCT period_12m) = 4
),

-- Reference bmi/weight at period -1
ref_bmi_weight AS (
    SELECT 
        person_id,
        AVG(bmi_visit) AS bmi_ref,
        AVG(weight_kg_visit) AS weight_kg_ref
    FROM 
        semaglutide_study_period_w_bmi
    WHERE
        period_12m = '-1'
    GROUP BY
        person_id
),


-- add concurrent meds information

/* Primary antihypertensive prescription */
anti_HTN_prescriptions_in_range AS (
    SELECT 
        s.person_id,
        ca.ancestor_concept_id AS ingredient_concept_id,
        CAST(de.drug_exposure_start_datetime AS DATE) AS drug_exposure_start_date,
        CASE 
            WHEN CAST(de.drug_exposure_start_datetime AS DATE) >= s.semaglutide_initiate_date_minus_6m AND CAST(de.drug_exposure_start_datetime AS DATE) < s.semaglutide_initiate_date THEN TRUE
            ELSE FALSE
        END AS pre_index,
        CASE 
            WHEN CAST(de.drug_exposure_start_datetime AS DATE) >= s.semaglutide_initiate_date AND CAST(de.drug_exposure_start_datetime AS DATE) <= s.semaglutide_initiate_date_plus_6m THEN TRUE
            ELSE FALSE
        END AS post_index
    FROM PARQUET_SCAN('/home/jupyter/2583347-data/drug_exposure/*.parquet') de
    INNER JOIN PARQUET_SCAN('/home/jupyter/2583347-data/concept_ancestor/*.parquet') ca
        ON ca.descendant_concept_id = de.drug_concept_id
    INNER JOIN PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_study_period.parquet') s
        ON de.person_id = s.person_id
    WHERE ca.ancestor_concept_id IN (
        -- THZ
        1395058, 974166, 978555, 907013, 
        -- ACEI
        1335471, 1340128, 1341927, 1363749, 1308216, 1310756, 1373225, 1331235, 1334456, 1342439,
        -- ARB
        40235485, 1351557, 1346686, 1347384, 1367500, 40226742, 1317640, 1308842,
        -- dCCB
        1332418, 1353776, 1326012, 1318137, 1318853, 1319880, 
        -- ndCCB
        1328165, 1307863
    ) AND CAST(de.drug_exposure_start_datetime AS DATE) BETWEEN s.semaglutide_initiate_date_minus_6m AND s.semaglutide_initiate_date_plus_6m
),

anti_HTN_ingredient_count AS (
    SELECT 
        person_id,
        COUNT(DISTINCT ingredient_concept_id) AS anti_HTN_total_ingredient_count,
        COUNT(DISTINCT CASE WHEN pre_index = TRUE THEN ingredient_concept_id END) AS anti_HTN_pre_index_ingredient_count,
        COUNT(DISTINCT CASE WHEN post_index = TRUE THEN ingredient_concept_id END) AS anti_HTN_post_index_ingredient_count
    FROM anti_HTN_prescriptions_in_range
    GROUP BY person_id
),

/* Primary antihyperlipidemic prescription */
anti_hyperlipidemic_prescriptions_in_range AS (
    SELECT 
        s.person_id,
        ca.ancestor_concept_id AS ingredient_concept_id,
        CAST(de.drug_exposure_start_datetime AS DATE) AS drug_exposure_start_date,
        CASE 
            WHEN CAST(de.drug_exposure_start_datetime AS DATE) >= s.semaglutide_initiate_date_minus_6m AND CAST(de.drug_exposure_start_datetime AS DATE) < s.semaglutide_initiate_date THEN TRUE
            ELSE FALSE
        END AS pre_index,
        CASE 
            WHEN CAST(de.drug_exposure_start_datetime AS DATE) >= s.semaglutide_initiate_date AND CAST(de.drug_exposure_start_datetime AS DATE) <= s.semaglutide_initiate_date_plus_6m THEN TRUE
            ELSE FALSE
        END AS post_index
    FROM PARQUET_SCAN('/home/jupyter/2583347-data/drug_exposure/*.parquet') de
    INNER JOIN PARQUET_SCAN('/home/jupyter/2583347-data/concept_ancestor/*.parquet') ca
        ON ca.descendant_concept_id = de.drug_concept_id
    INNER JOIN PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_study_period.parquet') s
        ON de.person_id = s.person_id
    WHERE ca.ancestor_concept_id IN (
        -- Statins
        1510813, -- rosuvastatin
        1539403, -- simvastatin
        1549686, -- fluvastatin
        1551860, -- pravastatin
        1592085, -- lovastatin
        1545958, -- atorvastatin
        40165636 -- pitavastatin
    ) AND CAST(de.drug_exposure_start_datetime AS DATE) BETWEEN s.semaglutide_initiate_date_minus_6m AND s.semaglutide_initiate_date_plus_6m
),

anti_hyperlipidemic_ingredient_count AS (
    SELECT 
        person_id,
        COUNT(DISTINCT ingredient_concept_id) AS anti_hyperlipidemic_total_ingredient_count,
        COUNT(DISTINCT CASE WHEN pre_index = TRUE THEN ingredient_concept_id END) AS anti_hyperlipidemic_pre_index_ingredient_count,
        COUNT(DISTINCT CASE WHEN post_index = TRUE THEN ingredient_concept_id END) AS anti_hyperlipidemic_post_index_ingredient_count
    FROM anti_hyperlipidemic_prescriptions_in_range
    GROUP BY person_id
),

/* JOIN anti_HTN_ingredient_count and anti_hyperlipidemic_ingredient_count */
concurrent_meds AS (
    SELECT 
        s.person_id,
        COALESCE(a.anti_HTN_total_ingredient_count, 0) AS anti_HTN_total_ingredient_count,
        COALESCE(a.anti_HTN_pre_index_ingredient_count, 0) AS anti_HTN_pre_index_ingredient_count,
        COALESCE(a.anti_HTN_post_index_ingredient_count, 0) AS anti_HTN_post_index_ingredient_count,
        COALESCE(b.anti_hyperlipidemic_total_ingredient_count, 0) AS anti_hyperlipidemic_total_ingredient_count,
        COALESCE(b.anti_hyperlipidemic_pre_index_ingredient_count, 0) AS anti_hyperlipidemic_pre_index_ingredient_count,
        COALESCE(b.anti_hyperlipidemic_post_index_ingredient_count, 0) AS anti_hyperlipidemic_post_index_ingredient_count
    FROM PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_study_period.parquet') s
    LEFT JOIN anti_HTN_ingredient_count a
        ON s.person_id = a.person_id
    LEFT JOIN anti_hyperlipidemic_ingredient_count b
        ON s.person_id = b.person_id
), 

-- Final Baseline Cohort
baseline_cohort AS (
    SELECT 
        sp.*,
        ss.semaglutide_order_sum,
        ss.first_order_route,
        ss.last_semaglutide_prescribe_date,
        ss.semaglutide_initiate_date,
        ss.semaglutide_initiate_date_minus_24m,
        ss.semaglutide_initiate_date_minus_12m,
        ss.semaglutide_initiate_date_plus_12m,
        ss.semaglutide_initiate_date_plus_24m,
        ss.semaglutide_initiate_date_minus_6m,
        ss.semaglutide_initiate_date_plus_6m,
        ss.active_semaglutide_order_count_in_period_1,
        t.first_t2dm_diag_date,
        t.last_t2dm_diag_date,
        CASE 
            WHEN t.first_t2dm_diag_date <= ss.semaglutide_initiate_date THEN 1.0 
            ELSE 0.0 
        END AS had_t2dm_diag_before_initiation,
        CASE 
            WHEN t.first_t2dm_diag_date IS NOT NULL THEN 1.0 
            ELSE 0.0 
        END AS had_t2dm_diag,
        DATE_DIFF('year', p.birth_date, ss.semaglutide_initiate_date) AS age_at_semaglutide_initiate,
        DATE_DIFF('year', p.birth_date, t.first_t2dm_diag_date) AS age_at_first_t2dm_diag,
        p.gender,
        p.race_ethn,
        p.birth_date,
        i.weight_kg_ref,
        i.bmi_ref,
        COALESCE(cm.anti_HTN_total_ingredient_count, 0) AS anti_HTN_total_ingredient_count,
        COALESCE(cm.anti_HTN_pre_index_ingredient_count, 0) AS anti_HTN_pre_index_ingredient_count,
        COALESCE(cm.anti_HTN_post_index_ingredient_count, 0) AS anti_HTN_post_index_ingredient_count,
        COALESCE(cm.anti_hyperlipidemic_total_ingredient_count, 0) AS anti_hyperlipidemic_total_ingredient_count,
        COALESCE(cm.anti_hyperlipidemic_pre_index_ingredient_count, 0) AS anti_hyperlipidemic_pre_index_ingredient_count,
        COALESCE(cm.anti_hyperlipidemic_post_index_ingredient_count, 0) AS anti_hyperlipidemic_post_index_ingredient_count,
        CASE WHEN COALESCE(cm.anti_HTN_total_ingredient_count, 0) > 0 THEN  1.0 ELSE 0.0
        END AS has_concurrent_anti_HTN_meds,
        CASE WHEN COALESCE(cm.anti_hyperlipidemic_total_ingredient_count, 0) > 0 THEN  1.0 ELSE 0.0
        END AS has_concurrent_anti_hyperlipidemic_meds
    FROM 
        selected_patients_w_all_periods sp
    LEFT JOIN
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_study_period.parquet') ss
        ON sp.person_id = ss.person_id
    LEFT JOIN
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/selected_patients_w_t2dm.parquet') t
        ON sp.person_id = t.person_id
    LEFT JOIN
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/selected_patients.parquet') p 
        ON sp.person_id = p.person_id
    LEFT JOIN
        ref_bmi_weight i ON sp.person_id = i.person_id
    LEFT JOIN
        concurrent_meds cm ON sp.person_id = cm.person_id
)

SELECT * FROM baseline_cohort

)
TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/baseline_cohort.parquet' (FORMAT PARQUET);


