COPY (

/*  Semaglutide related prescriptions */
-- Raw semaglutide prescriptions
WITH semaglutide_raw AS (
    SELECT 
        de.person_id,
        de.drug_exposure_id,
        de.drug_concept_id,
        c.concept_name,
        de.route_concept_id,
        COALESCE(
            -- use drug start date in sentara (datetime all NULL in sentara) ;  drug start datetime in YNHH (date all NULL in sentara)
            CAST(de.drug_exposure_start_datetime AS DATE), 
            CAST(vo.visit_start_datetime AS DATE)
        ) AS start_date_comb,
        -- IF drug_source_value contains 'tablet'/'oral'/'rybelsus', then the route is oral
        CASE
            WHEN LOWER(de.drug_source_value_name) LIKE '%tablet%' OR 
                 LOWER(de.drug_source_value_name) LIKE '%oral%'  OR 
                LOWER(de.drug_source_value_name) LIKE '%rybelsus%' THEN 'Oral'
            ELSE 'Subcutaneous'
        END AS route_extract
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

-- Process semaglutide data with combined start date
semaglutide AS (
    SELECT 
        *,
        DENSE_RANK() OVER (
            PARTITION BY person_id 
            ORDER BY start_date_comb
        ) AS semaglutide_order,
        MIN(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS first_semaglutide_prescribe_date,
        MIN(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS semaglutide_initiate_date,
        MAX(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS last_semaglutide_prescribe_date,
        CAST(MIN(start_date_comb) OVER (PARTITION BY person_id) - INTERVAL '24 months' AS DATE) AS semaglutide_initiate_date_minus_24m,
        CAST(MIN(start_date_comb) OVER (PARTITION BY person_id) - INTERVAL '12 months' AS DATE) AS semaglutide_initiate_date_minus_12m,
        CAST(MIN(start_date_comb) OVER (PARTITION BY person_id) + INTERVAL '12 months' AS DATE) AS semaglutide_initiate_date_plus_12m,
        CAST(MIN(start_date_comb) OVER (PARTITION BY person_id) + INTERVAL '24 months' AS DATE) AS semaglutide_initiate_date_plus_24m,
        CAST(MIN(start_date_comb) OVER (PARTITION BY person_id) - INTERVAL '6 months' AS DATE) AS semaglutide_initiate_date_minus_6m,
        CAST(MIN(start_date_comb) OVER (PARTITION BY person_id) + INTERVAL '6 months' AS DATE) AS semaglutide_initiate_date_plus_6m,
        COUNT(drug_exposure_id) OVER (
            PARTITION BY person_id
        ) AS semaglutide_order_sum,
        FIRST_VALUE(route_extract) OVER (
            PARTITION BY person_id
            ORDER BY start_date_comb
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_order_route
    FROM 
        semaglutide_raw
    WHERE
        start_date_comb IS NOT NULL
), 

-- Define study periods
semaglutide_study_period AS (
    SELECT DISTINCT
        person_id,
        semaglutide_order_sum,
        first_order_route,
        last_semaglutide_prescribe_date,
        semaglutide_initiate_date,
        semaglutide_initiate_date_minus_24m,
        semaglutide_initiate_date_minus_12m,
        semaglutide_initiate_date_plus_12m,
        semaglutide_initiate_date_plus_24m,
        semaglutide_initiate_date_minus_6m,
        semaglutide_initiate_date_plus_6m,
        SUM(CASE WHEN 
			semaglutide_initiate_date_plus_12m <= start_date_comb AND 
			start_date_comb < semaglutide_initiate_date_plus_24m
			THEN 1 ELSE 0 END) AS active_semaglutide_order_count_in_period_1
    FROM 
        semaglutide 
    GROUP BY 
        person_id,
        semaglutide_order_sum,
        first_order_route,
        last_semaglutide_prescribe_date,
        semaglutide_initiate_date,
        semaglutide_initiate_date_minus_24m,
        semaglutide_initiate_date_minus_12m,
        semaglutide_initiate_date_plus_12m,
        semaglutide_initiate_date_plus_24m,
        semaglutide_initiate_date_minus_6m,
        semaglutide_initiate_date_plus_6m
)

SELECT * FROM semaglutide_study_period

)
TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p4_12m/semaglutide_study_period.parquet' (FORMAT PARQUET);



