COPY (
-- T2DM History Information
WITH selected_patients_w_t2dm AS (
    SELECT 
        person_id,
        MIN(condition_start_date) AS first_t2dm_diag_date,
        MAX(condition_start_date) AS last_t2dm_diag_date
    FROM (
        SELECT 
            co.person_id,
            co.condition_concept_id,
            CAST(co.condition_start_datetime AS DATE) AS condition_start_date,
            co.condition_status_source_value
        FROM 
            PARQUET_SCAN('/home/jupyter/2583347-data/condition_occurrence/*.parquet')  co
        WHERE
            co.person_id IN (SELECT person_id FROM PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions/interim_tables/p4_12m/semaglutide_study_period.parquet'))
            AND co.condition_concept_id IN (
                SELECT descendant_concept_id 
                FROM PARQUET_SCAN('/home/jupyter/2583347-data/concept_ancestor/*.parquet')
                WHERE ancestor_concept_id IN (201826)
            )
            AND CAST(co.condition_start_datetime AS DATE) >= DATE '2015-01-01'-- From 2015
    ) t2dm
    GROUP BY 
        person_id
)

SELECT * FROM selected_patients_w_t2dm
)

TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions/interim_tables/p4_12m/selected_patients_w_t2dm.parquet' (FORMAT PARQUET);


