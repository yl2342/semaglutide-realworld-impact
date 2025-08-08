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
            WHEN s.semaglutide_initiate_date_minus_12m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_minus_6m THEN '-2'
            WHEN s.semaglutide_initiate_date_minus_6m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date THEN '-1'
            WHEN s.semaglutide_initiate_date <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_6m THEN '0'
            WHEN s.semaglutide_initiate_date_plus_6m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_12m THEN '1'
            WHEN s.semaglutide_initiate_date_plus_12m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_18m THEN '2'
            ELSE NULL 
        END AS period_6m
    FROM 
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/semaglutide_study_period.parquet') s 
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/semaglutide_initiate_patients_first_last_visit_date.parquet') fl      
        ON s.person_id = fl.person_id
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/bmiw_visit.parquet') b
        ON s.person_id = b.person_id
    WHERE 
        (s.semaglutide_initiate_date_plus_18m <= fl.last_visit_date)  -- Ensure in observation period
        AND (fl.first_visit_date <= s.semaglutide_initiate_date_minus_12m) -- Ensure in observation period
),

-- Select patients with BMI/weight data for all 5 periods
selected_patients_w_all_periods AS (
    SELECT 
        person_id,
        COUNT(DISTINCT period_6m) AS period_6m_count,
        MAX(semaglutide_initiate_date_plus_18m) AS study_period_end,
        MAX(last_visit_date) AS observe_period_end,
        MIN(semaglutide_initiate_date_minus_12m) AS study_period_start,
        MIN(first_visit_date) AS observe_period_start
    FROM 
        semaglutide_study_period_w_bmi
    GROUP BY 
        person_id
    HAVING 
        COUNT(DISTINCT period_6m) = 5
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
        period_6m = '-1'
    GROUP BY
        person_id
),

-- Final Baseline Cohort
baseline_cohort AS (
    SELECT 
        sp.*,
        ss.semaglutide_order_sum,
        ss.semaglutide_initiate_date,
        ss.semaglutide_initiate_date_minus_6m,
        ss.semaglutide_initiate_date_minus_12m,
        ss.semaglutide_initiate_date_plus_6m,
        ss.semaglutide_initiate_date_plus_12m,
        ss.semaglutide_initiate_date_plus_18m,
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
        i.bmi_ref
    FROM 
        selected_patients_w_all_periods sp
    LEFT JOIN
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/semaglutide_study_period.parquet') ss
        ON sp.person_id = ss.person_id
    LEFT JOIN
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/selected_patients_w_t2dm.parquet') t
        ON sp.person_id = t.person_id
    LEFT JOIN
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/selected_patients.parquet') p 
        ON sp.person_id = p.person_id
    LEFT JOIN
        ref_bmi_weight i ON sp.person_id = i.person_id
)

SELECT * FROM baseline_cohort

)
TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p5_6m/baseline_cohort.parquet' (FORMAT PARQUET);

