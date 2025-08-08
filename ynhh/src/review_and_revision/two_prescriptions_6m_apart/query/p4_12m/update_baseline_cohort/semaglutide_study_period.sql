COPY (

/*  Semaglutide related prescriptions */
-- Raw semaglutide prescriptions
WITH semaglutide_raw AS (
    SELECT 
        de.person_id,
        de.drug_exposure_id,
        de.drug_concept_id,
        c.concept_name,
        COALESCE(
            -- use drug start date in sentara (datetime all NULL in sentara) ;  drug start datetime in YNHH (date all NULL in sentara)
            CAST(de.drug_exposure_start_datetime AS DATE), 
            CAST(vo.visit_start_datetime AS DATE)
        ) AS start_date_comb
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/drug_exposure/*.parquet') de
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/2583347-data/concept/*.parquet') c ON de.drug_concept_id = c.concept_id
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo ON de.person_id = vo.person_id 
            AND de.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        de.drug_concept_id IN (
            SELECT 
                descendant_concept_id 
            FROM  
                PARQUET_SCAN('/home/jupyter/2583347-data/concept_ancestor/*.parquet')
            WHERE 
                ancestor_concept_id IN (793143)
        ) OR
        LOWER(de.drug_source_value_name) LIKE '%semaglutide%' OR
        LOWER(de.drug_source_value_name) LIKE '%ozempic%' OR
        LOWER(de.drug_source_value_name) LIKE '%wegovy%' OR
        LOWER(de.drug_source_value_name) LIKE '%rybelsus%'
),

-- Process semaglutide data with combined start date and calculate days from first prescription
semaglutide AS (
    SELECT 
        s.*,
        DENSE_RANK() OVER (
            PARTITION BY person_id 
            ORDER BY start_date_comb
        ) AS semaglutide_order,
        MIN(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS first_semaglutide_prescribe_date,
        MAX(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS last_semaglutide_prescribe_date,
        COUNT(drug_exposure_id) OVER (
            PARTITION BY person_id
        ) AS semaglutide_order_sum,
        DATE_DIFF('day', 
            MIN(start_date_comb) OVER (PARTITION BY person_id), 
            start_date_comb
        ) AS days_from_first_prescription
    FROM 
        semaglutide_raw s
    WHERE
        start_date_comb IS NOT NULL
), 

-- Define study periods
semaglutide_study_period AS (
    SELECT DISTINCT
        s.person_id,
        s.semaglutide_order_sum,
        s.last_semaglutide_prescribe_date,
        CAST(s.first_semaglutide_prescribe_date AS DATE) AS semaglutide_initiate_date,
        CAST(s.first_semaglutide_prescribe_date - INTERVAL '24 months' AS DATE) AS semaglutide_initiate_date_minus_24m,
        CAST(s.first_semaglutide_prescribe_date - INTERVAL '12 months' AS DATE) AS semaglutide_initiate_date_minus_12m,
        CAST(s.first_semaglutide_prescribe_date + INTERVAL '12 months' AS DATE) AS semaglutide_initiate_date_plus_12m,
        CAST(s.first_semaglutide_prescribe_date + INTERVAL '24 months' AS DATE) AS semaglutide_initiate_date_plus_24m
    FROM 
        semaglutide s
    WHERE
        s.semaglutide_order_sum >= 2  -- Require at least 2 semaglutide orders
        AND DATE_DIFF('month', s.first_semaglutide_prescribe_date, s.last_semaglutide_prescribe_date) >= 6  -- Ensure at least 6 months between first and last prescription
)

SELECT * FROM semaglutide_study_period

)
TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions_6m_apart/interim_tables/p4_12m/semaglutide_study_period.parquet' (FORMAT PARQUET);

