COPY (

-- Patient Demographics
WITH selected_patients AS (
    SELECT 
        person_id AS person_id,
        CAST(birth_datetime AS DATE) AS birth_date, 
        CASE 
            WHEN gender_source_value = 'Male' THEN 'Male'
            WHEN gender_source_value = 'Female' THEN 'Female' 
            ELSE 'Other/Unknown'
        END AS gender,
        CASE 
            WHEN  ethnicity_source_value IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Hispanic/Latino'
            WHEN  race_source_value = 'White' AND ethnicity_source_value NOT IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Non-Hispanic White'
            WHEN  race_source_value = 'Black or African American' AND ethnicity_source_value NOT IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Non-Hispanic Black'
            WHEN  race_source_value = 'Asian' AND ethnicity_source_value NOT IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Non-Hispanic Asian'
            ELSE 'Other/Unknown' 
        END AS race_ethn
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/person/*.parquet')
    WHERE
        person_id IN (
            SELECT person_id FROM PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/by_route_oral/interim_tables/p4_12m/semaglutide_study_period.parquet'))
)

SELECT * FROM  selected_patients

)
TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/by_route_oral/interim_tables/p4_12m/selected_patients.parquet' (FORMAT PARQUET);



