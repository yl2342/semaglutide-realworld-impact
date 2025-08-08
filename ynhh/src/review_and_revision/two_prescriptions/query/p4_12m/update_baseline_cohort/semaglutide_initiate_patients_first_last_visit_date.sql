COPY (

-- First/last visit dates (2015 - 2025-05-01)
WITH semaglutide_initiate_patients_first_last_visit_date AS (
    SELECT 
        person_id,
        CAST(MIN(visit_start_datetime) AS DATE) AS first_visit_date,
        CAST(MAX(visit_start_datetime) AS DATE) AS last_visit_date
    FROM (
        SELECT *
        FROM PARQUET_SCAN('/home/jupyter/2583347-data/visit_occurrence/*.parquet') 
        WHERE CAST(visit_start_datetime AS DATE) >= DATE '2015-01-01'  -- From 2015
        AND CAST(visit_start_datetime AS DATE) <= DATE '2025-05-01'  -- until 2025-05-01
        AND person_id IN (SELECT person_id FROM PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions/interim_tables/p4_12m/semaglutide_study_period.parquet'))
    ) tmp
    GROUP BY 
        person_id
)

SELECT * FROM semaglutide_initiate_patients_first_last_visit_date
)

TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions/interim_tables/p4_12m/semaglutide_initiate_patients_first_last_visit_date.parquet' (FORMAT PARQUET);



